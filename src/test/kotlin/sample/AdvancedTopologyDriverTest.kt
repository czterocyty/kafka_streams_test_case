package sample

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.flatMap
import arrow.core.getOrHandle
import arrow.core.lastOrNone
import arrow.core.left
import arrow.core.orNull
import arrow.core.right
import arrow.core.toOption
import assertk.Assert
import assertk.assertAll
import assertk.assertions.containsExactly
import assertk.assertions.isEqualTo
import helios.core.Json
import helios.instances.ListDecoderInstance
import helios.instances.ListEncoderInstance
import helios.instances.decoder
import helios.instances.encoder
import helios.json
import helios.typeclasses.Decoder
import helios.typeclasses.Encoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import sample.Dsl.Companion.STORE_MAPPING
import java.nio.charset.Charset
import java.util.*

typealias AggregationR = Either<List<RoutedToParent>, Option<Parent>>

internal class AdvancedTopologyDriverTest {

    @Test
    fun `initial parent`() {
        val kids = listOf(Child(TID1))

        val parents = aggregate(kids)

        assertk.assert(parents).containsExactly(
            TID1 to Parent(
                TID1,
                listOf(Child(TID1)),
                generation = 1
            )
        )
    }

    @Test
    fun `append next kid`() {
        val kids = listOf(
            Child(TID2),
            Child(TID2, other = listOf(TID2))
        )

        val parents = aggregate(kids)

        assertk.assert(parents.lastParent(TID2)).contains(
            TID2 to Parent(
                TID2,
                listOf(
                    Child(TID2),
                    Child(TID2, other = listOf(TID2))
                ),
                generation = 2
            )
        )
    }

    @Test
    @Disabled("output records come not in order as stated in TestTopologyDriver")
    fun `append a kid that refers to other parent`() {
        val kids = listOf(
            Child(TID2),
            Child(TID1, listOf(TID1, TID2))
        )

        val parents = aggregate(kids)

        assertAll {
            assertk.assert(parents.lastParent(TID2)).contains(
                TID2 to null
            )

            assertk.assert(parents.lastParent(TID1)).contains(
                TID1 to Parent(
                    TID1,
                    listOf(
                        Child(TID1, listOf(TID1, TID2)),
                        Child(TID2)
                    ),
                    generation = 2
                )
            )
        }
    }

    @Test
    fun `append a kid that refers to other parent, check state store`() {
        val kids = listOf(
            Child(TID2),
            Child(TID1, listOf(TID1, TID2))
        )

        val stateStore = aggregateAndLookAtStateStore(kids)

        assertAll {
            assertk.assert(stateStore[TID1]).isEqualTo(
                Parent(
                    TID1,
                    listOf(
                        Child(TID1, listOf(TID1, TID2)),
                        Child(TID2)
                    ),
                    generation = 2
                ).toOption().right()
            )
        }
    }

    @Test
    fun `append late kid to master`() {
        val tp1 = Child(TID1, other = listOf(TID1, TID2))
        val tp2 = Child(TID2)

        val parents = aggregate(listOf(tp1, tp2))

        assertAll {
            assertk.assert(parents.lastParent(TID2)).contains(TID2 to null)

            assertk.assert(parents.lastParent(TID1)).contains(
                TID1 to Parent(
                    TID1,
                    listOf(tp1, Child(TID2)),
                    generation = 2
                )
            )
        }
    }

    @Test
    @Disabled("output records come not in order as stated in TestTopologyDriver")
    fun `append additional to master once mapping is available`() {
        val tp1 = Child(TID1)
        val tp2 = Child(TID2)
        val tp3 = Child(TID1, listOf(TID1, TID2, TID3))

        val parents = aggregate(listOf(tp1, tp2, tp3))

        assertAll {
            assertk.assert(parents.lastParent(TID1)).contains(
                TID1 to Parent(
                    TID1,
                    listOf(tp1, tp3, Child(TID2)),
                    generation = 3
                )
            )
            assertk.assert(parents.lastParent(TID2)).contains(TID2 to null)
        }
    }

    @Test
    fun `append additional to master once mapping is available, check state store`() {
        val tp1 = Child(TID1)
        val tp2 = Child(TID2)
        val tp3 = Child(TID1, listOf(TID1, TID2, TID3))

        val parents = aggregateAndLookAtStateStore(listOf(tp1, tp2, tp3))

        assertAll {
            assertk.assert(parents[TID1]).isEqualTo(
                Parent(
                    TID1,
                    listOf(tp1, tp3, Child(TID2)),
                    generation = 3
                ).toOption().right()
            )
        }
    }

    private fun aggregate(children: List<Child>): List<Pair<String, Parent?>> {
        return MockedStreams.builder {
            Dsl().create()
        }
            .input(
                Dsl.TOPIC_CHILD,
                Serdes.String(),
                Serdes.String(), children.map {
                it.id to with(Child.encoder()) { it.encode().noSpaces() }
            })
            .output(
                Dsl.TOPIC_PARENT,
                Serdes.String(),
                Serdes.String(), 10)
            .map { it.first to Json.parseFromString(it.second)
                .flatMap { json -> Parent
                    .decoder().decode(json)
                }
                .orNull()
            }
    }

    private fun aggregateAndLookAtStateStore(children: List<Child>): Map<String, AggregationR> {
        return MockedStreams.builder {
            Dsl().create()
        }.input(
            Dsl.TOPIC_CHILD,
            Serdes.String(),
            Serdes.String(),
            children.map { it.id to with(Child.encoder()) { it.encode().noSpaces() } }
        ).stateTable(Dsl.STORE_PARENTS)
    }

    private fun List<Pair<String, Parent?>>.lastParent(id: String): Option<Pair<String, Parent?>> {
        return this
            .filter { it.first == id }
            .lastOrNone()
    }

    companion object {
        const val TID1 = "id1"
        const val TID2 = "id2"
        const val TID3 = "id3"
    }
}

fun <A> Assert<Option<A>>.contains(a: A) {
    this.assert(this.actual).isEqualTo(Option.just(a))
}

object MockedStreams {

    fun builder(topology: () -> Topology) = Builder(topology)

    data class Builder(val topology: () -> Topology,
                       val configuration: Properties = Properties(),
                       val stateStores: List<String> = emptyList(),
                       val inputs: List<ConsumerRecord<ByteArray, ByteArray>> = emptyList()) {

        fun <K, V> input(topic: String, key: Serde<K>, value: Serde<V>, records: List<Pair<K, V>>): Builder {
            val keySer = key.serializer()
            val valSer = value.serializer()
            val factory = ConsumerRecordFactory<K, V>(keySer, valSer)

            return this.copy(inputs = records.map { (k, v) -> factory.create(topic, k, v) })
        }

        fun <K, V> output(topic: String, key: Serde<K>, value: Serde<V>, size: Int): List<Pair<K, V>> {
            return withProcessedDriver { driver ->
                (0 until size).flatMap {
                    driver.readOutput(topic, key.deserializer(), value.deserializer())
                        .toOption()
                        .map { record -> record.key() to record.value() }
                        .toList()
                }
            }
        }

        fun <K, V> stateTable(name: String): Map<K, V> = withProcessedDriver { driver ->
            driver.getKeyValueStore<K, V>(name).all().use { records: KeyValueIterator<K, V> ->
                records.asSequence().map { kv ->
                    kv.key to kv.value
                }.toMap()
            }
        }

        private fun stream(): TopologyTestDriver {
            val props = Properties()
            props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-${UUID.randomUUID()}"
            props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
            props.putAll(this.configuration)
            return TopologyTestDriver(topology(), props)
        }

        private fun <T> withProcessedDriver(f: (TopologyTestDriver) -> T): T {
            val driver = stream()
            driver.use {
                this.inputs.forEach( driver::pipeInput )
                return f(driver)
            }
        }
    }
}

class PoisonPillRouter :
    ValueTransformerWithKey<String, Child, Either<RoutedToParent, Child>> {
    private lateinit var poisonPillStore: KeyValueStore<String, PoisonPill>

    @Suppress("UNCHECKED_CAST")
    override fun init(context: ProcessorContext) {
        poisonPillStore =
            context.getStateStore(STORE_MAPPING)
                as KeyValueStore<String, PoisonPill>
    }

    override fun transform(key: String, child: Child): Either<RoutedToParent, Child> {
        val mapping = poisonPillStore.get(key)
        return mapping.toOption()
            .filter { m -> m.orphanId == key }
            .fold({ child.right() }) { m ->
                RoutedToParent(m.parentId, child).left()
            }
    }

    override fun close() {
        // must be so
    }

    companion object {
        fun supplier(): ValueTransformerWithKeySupplier<String, Child,
            Either<RoutedToParent, Child>> {
            return ValueTransformerWithKeySupplier { PoisonPillRouter() }
        }
    }
}

class PoisonPillSaver :
    ValueTransformerWithKey<String, PoisonPill, Child> {

    private lateinit var poisonPillStore: KeyValueStore<String, PoisonPill>

    @Suppress("UNCHECKED_CAST")
    override fun init(context: ProcessorContext) {
        poisonPillStore =
            context.getStateStore(STORE_MAPPING) as KeyValueStore<String, PoisonPill>
    }

    override fun transform(key: String, poisonPill: PoisonPill): Child {
        poisonPillStore.put(key, poisonPill)
        return Child(poisonPill.orphanId, masterId = poisonPill.parentId.toOption())
    }

    override fun close() {
        // must be so
    }

    companion object {
        fun supplier():
            ValueTransformerWithKeySupplier<String, PoisonPill, Child> {

            return ValueTransformerWithKeySupplier { PoisonPillSaver() }
        }
    }
}


class Dsl {

    private val builder = StreamsBuilder()

    fun create(): Topology {
        createPoisonPillMappingStore()

        val kids = builder.stream(TOPIC_CHILD, Consumed.with(Serdes.String(), SERDE_CHILD))

        val chuckyDolls = builder.stream(TOPIC_POISON_PILL, Consumed.with(Serdes.String(), SERDE_POISON_PILL))
            .transformValues(
                PoisonPillSaver.supplier(),
                STORE_MAPPING
            )

        val (kidsToMove, localKids) = kids
            .transformValues(PoisonPillRouter.supplier(), STORE_MAPPING)
            .branchByEither()

        val orphans = kidsToMove
            .map { _, kid ->
                KeyValue(kid.parentId, kid)
            }
            .through(TOPIC_ROUTED_TO_PARENT, Produced.with(Serdes.String(), SERDE_ROUTED))
            .mapValues { _, routed -> routed.orphan }

        localKids
            .flatMap { _, tp ->
                tp.toPoisonPills().map { pp ->
                    KeyValue(pp.orphanId, pp)
                }
            }
            .to(TOPIC_POISON_PILL, Produced.with(Serdes.String(), SERDE_POISON_PILL))

        val finalKidsForAggregation = localKids
            .merge(chuckyDolls)
            .merge(orphans)
        val parents = aggregatingStream(finalKidsForAggregation)

        parents.to(TOPIC_PARENT, Produced.with(Serdes.String(), SERDE_PARENT))

        return builder.build()
    }

    private fun createPoisonPillMappingStore() {
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_MAPPING),
                Serdes.String(), SERDE_POISON_PILL
            )
        )
    }

    private fun aggregatingStream(combined: KStream<String, Child>): KStream<String, Parent> {
        val store = Materialized
            .`as`<String, AggregationR>(
                Stores.persistentKeyValueStore(STORE_PARENTS)
            )
            .withKeySerde(Serdes.String())
            .withValueSerde(SERDE_AGG)
            .withCachingEnabled()

        val (parentsToKill, parentsToSave) = combined
            .groupByKey()
            .aggregate(
                { Option.empty<Parent>().right() },
                { id: String, child: Child, aggregate: AggregationR ->
                    aggregate.flatMap { parentOpt ->
                        parentOpt.fold({
                            val empty = Parent(id)
                            empty.join(child)
                        }) { parent ->
                            parent.join(child)
                        }
                    }
                },
                store
            )
            .toStream()
            .branchByEither()

        parentsToKill
            .map { key, _ -> KeyValue<String, Parent?>(key, null) }
            .to(TOPIC_PARENT, Produced.with(Serdes.String(), SERDE_PARENT))

        parentsToKill
            .flatMap { _, list ->
                list.map { child ->
                    KeyValue(child.parentId, child)
                }
            }
            .to(TOPIC_ROUTED_TO_PARENT, Produced.with(Serdes.String(), SERDE_ROUTED ))

        return parentsToSave
            .map { key, value -> KeyValue(key, value.orNull()!!) }
    }

    private fun <K, L, R> KStream<K, Either<L, R>>.mapLeft(): KStream<K, L> {
        return this
            .filter { _, either -> either.isLeft() }
            .mapValues { _, either -> (either as Either.Left).a }
    }

    private fun <K, L, R> KStream<K, Either<L, R>>.mapRight(): KStream<K, R> {
        return this
            .filter { _, either -> either.isRight() }
            .mapValues { _, either -> (either as Either.Right).b }
    }

    private fun <K, L, R> KStream<K, Either<L, R>>.branchByEither(): Tuple2<KStream<K, L>, KStream<K, R>> {
        val branches = this.branch(
            Predicate { _, e -> e != null && e.isLeft() },
            Predicate { _, e -> e != null && e.isRight() })

        return Tuple2(branches[0].mapLeft(), branches[1].mapRight())
    }


    companion object {
        val STORE_MAPPING = "mapping"
        val STORE_PARENTS = "parents"

        val TOPIC_PARENT = "parent"
        val TOPIC_CHILD = "child"
        val TOPIC_POISON_PILL = "poison_pill"
        val TOPIC_ROUTED_TO_PARENT = "routed"

        val SERDE_CHILD = serdeFor(Child.encoder(), Child.decoder())
        val SERDE_PARENT = serdeFor(Parent.encoder(), Parent.decoder())
        val SERDE_POISON_PILL = serdeFor(PoisonPill.encoder(), PoisonPill.decoder())
        val SERDE_ROUTED = serdeFor(RoutedToParent.encoder(), RoutedToParent.decoder())
        val SERDE_AGG: Serde<AggregationR> = serdeFor(
            Either.encoder(ListEncoderInstance(RoutedToParent.encoder()), Option.encoder(Parent.encoder())),
            Either.decoder(ListDecoderInstance(RoutedToParent.decoder()), Option.decoder(Parent.decoder()))
        )
    }
}

@json
data class Child(val id: String,
                 val other: List<String> = emptyList(),
                 val masterId: Option<String> = Option.empty()) {

    fun toPoisonPills(): List<PoisonPill> {
        return other
            .filterNot { it == this.id }
            .map { kid ->
                PoisonPill(kid, this.id)
            }
    }

    companion object
}

@json
data class Parent(val id: String,
                  val kids: List<Child> = emptyList(),
                  val generation: Int = 0) {

    fun join(kid: Child): AggregationR {
        return when (kid.masterId) {
            is Some -> this.kids.map { k ->
                RoutedToParent(kid.masterId.t, k)
            }.left()
            is None -> this.copy(
                kids = this.kids + kid,
                generation = generation + 1).toOption().right()
        }
    }

    companion object
}

@json
data class PoisonPill(val orphanId: String,
                      val parentId: String) {
    companion object
}

@json
data class RoutedToParent(val parentId: String,
                          val orphan: Child) {
    companion object
}

class HeliosDeserializer<T>(private val decoder: Decoder<T>) :
    Deserializer<T> {
    override fun deserialize(topic: String, data: ByteArray?): T? {
        return if (data == null || data.isEmpty()) {
            null
        } else {
            val str = data.toString(Charset.defaultCharset())
            Json.parseFromString(str)
                .flatMap { json ->
                    decoder.decode(json)
                }
                .getOrHandle { _ ->
                    null
                }
        }
    }
}

class HeliosSerializer<T>(private val encoder: Encoder<T>) :
    Serializer<T> {
    override fun serialize(topic: String, data: T?): ByteArray {
        return if (data == null) {
            ByteArray(0)
        } else {
            with(encoder) {
                data.encode()
            }.noSpaces().toByteArray()
        }
    }
}

fun <A> serdeFor(encoder: Encoder<A>, decoder: Decoder<A>): Serde<A> {
    return Serdes.serdeFrom(
        HeliosSerializer(encoder),
        HeliosDeserializer(decoder)
    )
}
