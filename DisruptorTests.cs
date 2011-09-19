using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using PicoIoc;
using Xunit;

namespace PicoFx.Disruptor {

	// ReSharper disable InconsistentNaming
	public class RingBufferTests {
		//readonly WaitStrategy _waitStrategy = new SpinWaitStrategy();
		readonly WaitStrategy _waitStrategy = new ThreadYieldWaitStrategy();

		public RingBufferTests() {
			Trace.Listeners.Add( new DefaultTraceListener() );
		}

		[Fact]
		public void DisruptorCanMakeConsumersWaitForProducers() {
			var sut = new Disruptor<int>( 16, _waitStrategy );
			int[] counters = new int[ 2 ];

			var t1 = new Thread( () => {
				Thread.Sleep( 100 );

				sut.Put( 9 );
				Thread.Sleep( 5 );
				sut.Put( 12 );
				sut.Commit();
				sut.Stop();
			} ) { Name = "Producer" };

			var t2 = new Thread( () => {
				Thread.Sleep( 50 );

				var batch = sut.ConsumeNextBatch();
				counters[ 0 ] += batch.Count();
				sut.Commit();

			} ) { Name = "Consumer" };

			var t3 = new Thread( () => {
				Thread.Sleep( 20 );

				var batch = sut.CleanNextBatch();
				counters[ 1 ] += batch.Count();
				sut.Commit();

			} ) { Name = "Cleaner" };

			t1.Start();
			t2.Start();
			t3.Start();

			t1.Join();
			t2.Join();
			t3.Join();

			Assert.Equal(2, counters[0]);
			Assert.Equal(2, counters[1]);
		}

		[Fact]
		public void Disruptor_can_make_many_producers_cooperate() {
			const int NumMessagesToproduce = 5000000;

			var buffer = new TracingRingBuffer<int>( 100000 );
			var sut = new Disruptor<int>( _waitStrategy, buffer );

			const int batchSize = 500;
			const int numberOfBatches = ( NumMessagesToproduce / batchSize ) / 10;

			Thread[] threads = new Thread[ 10 ];
			threads[ 0 ] = CreateSlowWorker( sut, "Producer 1", numberOfBatches, batchSize );
			threads[ 1 ] = CreateFastWorker( sut, "Producer 2", numberOfBatches, batchSize );
			threads[ 2 ] = CreateFastWorker( sut, "Producer 3", numberOfBatches, batchSize );
			threads[ 3 ] = CreateSlowWorker( sut, "Producer 4", numberOfBatches, batchSize );
			threads[ 4 ] = CreateFastWorker( sut, "Producer 5", numberOfBatches, batchSize );
			threads[ 5 ] = CreateSlowWorker( sut, "Producer 6", numberOfBatches, batchSize );
			threads[ 6 ] = CreateFastWorker( sut, "Producer 7", numberOfBatches, batchSize );
			threads[ 7 ] = CreateFastWorker( sut, "Producer 8", numberOfBatches, batchSize );
			threads[ 8 ] = CreateSlowWorker( sut, "Producer 9", numberOfBatches, batchSize );
			threads[ 9 ] = CreateSlowWorker( sut, "Producer 10", numberOfBatches, batchSize );

			var cleanerThread = new Thread( () => {
				int count = 0;

				while ( count < NumMessagesToproduce ) {
					var batch = sut.CleanNextBatch();
					int size = batch.Count();
					count += size;
					sut.Commit();
				}
				sut.Stop();
			} ) { Name = "Cleaner" };

			var stopwatch = new Stopwatch();
			stopwatch.Start();
			threads.Each( t => t.Start() );
			cleanerThread.Start();

			cleanerThread.Join();

			stopwatch.Stop();
			Trace.WriteLine( stopwatch.Elapsed );

			double tps = 6000000 / stopwatch.Elapsed.TotalSeconds;
			Trace.WriteLine( string.Format( "~{0:0} TPS", tps ) );
			Assert.True( tps > 1500000, "Running waaayy too slowly." );
			buffer.CheckAssumptions();
		}

		private static Thread CreateFastWorker( Disruptor<int> sut, string name, int numberOfBatches, int batchSize ) {
			return new Thread( () => {
				for ( int i = 0; i < numberOfBatches; i++ ) {
					try { sut.Put( 0.To( batchSize - 1 ) ); }
					finally { sut.Commit(); } // don't block any other workers
				}
			} ) { Name = name };
		}

		private static Thread CreateSlowWorker( Disruptor<int> sut, string name, int numberOfBatches, int batchSize ) {
			return new Thread( () => {
				for ( int i = 0; i < numberOfBatches; i++ ) {
					for ( int j = 0; j < batchSize; j++ ) {
						try { sut.Put( j ); }
						finally { sut.Commit(); } // don't block any other workers
					}
				}
			} ) { Name = name };
		}

		[Fact]
		public void Disruptor_is_fast_when_capacity_is_much_larger_than_batch_size() {
			var sut = new Disruptor<int>( 100000, _waitStrategy );
			const int numBatches = 6000;
			const int batchSize = 1000;

			var t1 = new Thread( () => {
				for ( int i = 0; i < numBatches; i++ ) {
					sut.Put( 0.To( batchSize - 1 ) );
					sut.Commit();
				}
			} ) { Name = "Producer" };

			var t2 = new Thread( () => {
				int count = 0;

				while ( count < numBatches * batchSize ) {
					var batch = sut.ConsumeNextBatch();
					int currentBatchSize = batch.Count();
					count += currentBatchSize;
					sut.Commit();
				}
			} ) { Name = "Consumer" };

			var t3 = new Thread( () => {
				int count = 0;

				while ( count < numBatches * batchSize ) {
					var batch = sut.CleanNextBatch();
					int currentBatchSize = batch.Count();
					count += currentBatchSize;
					//Trace.WriteLine("Cleaner: " + batchSize + " items");
					sut.Commit();
				}
			} ) { Name = "Cleaner" };

			var stopwatch = new Stopwatch();
			stopwatch.Start();
			t1.Start();
			t2.Start();
			t3.Start();

			t1.Join();
			t2.Join();
			t3.Join();

			stopwatch.Stop();
			Trace.WriteLine( stopwatch.Elapsed );
			double tps = numBatches * batchSize / stopwatch.Elapsed.TotalSeconds;
			Trace.WriteLine( string.Format( "~{0:0} TPS", tps ) );
			Assert.True( tps > 1500000, "Running waaayy too slowly." );
		}

		[Fact]
		public void RingBufferSizeSnapsToPowerOfTwo() {
			// Arrange
			var sut = new RingBuffer<int>( 15 );

			// Assert
			Assert.Equal( 16, sut.Size );
		}

		[Fact]
		public void RingBufferSizeSnapsToPowerOfTwo2() {
			// Arrange
			var sut = new RingBuffer<int>( 9 );

			// Assert
			Assert.Equal( 16, sut.Size );
		}

		[Fact]
		public void RingBufferSizeSnapsToPowerOfTwoIfGivenSizeIsPowerOfTwo() {
			// Arrange
			var sut = new RingBuffer<int>( 32 );

			// Assert
			Assert.Equal( 32, sut.Size );
		}

		[Fact]
		public void RingBufferCanWrapIndex() {
			// Arrange
			var sut = new RingBuffer<int>( 31 );

			sut[ 33 ] = 9384;

			Assert.Equal( 9384, sut[ 1 ] );
		}

		[Fact]
		public void ConsumerHotObservableCommitsAfterEachBatch() {
			// Arrange
			bool[] committed = new bool[ 1 ];
			int[] count = new int[ 1 ];

			var sut = new ConsumerHotObservable<int>( () => Enumerable.Range( 10, 20 ), () => committed[ 0 ] = true );

			// Act
			IDisposable subscription = sut.SubscribeOn( Scheduler.NewThread ).Subscribe( i => count[ 0 ]++ );

			Thread.Sleep( 10 );
			subscription.Dispose();

			// Assert
			Assert.True( committed[ 0 ] );
			Assert.True( count[ 0 ] >= 20 );
		}

	}
	// ReSharper restore InconsistentNaming

	public class TracingRingBuffer<T> : RingBuffer<T> {
		int[] positionsSetCount = new int[ 5000000 ];

		public TracingRingBuffer( int size ) : base( size ) { }

		public override T this[ long index ] {
			get {
				return base[ index ];
			}
			set {
				Interlocked.Increment( ref positionsSetCount[ index ] );
				base[ index ] = value;
			}
		}

		public void CheckAssumptions() {
			// No two producers are setting the same value
			Assert.True( positionsSetCount.All( p => p < 2 ) );

			// No holes in the sequence
			for ( int i = 0; i < positionsSetCount.Length; i++ ) {
				if ( i > 0 && positionsSetCount[ i ] > 0 )
					Assert.True( positionsSetCount[ i - 1 ] > 0 );
			}
		}
	}
}