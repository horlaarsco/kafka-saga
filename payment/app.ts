import { initConsumer } from './kafka/consumer'

initConsumer().catch((e: unknown) => console.log(e))
