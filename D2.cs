using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using PicoFx.Disruptor;
using Xunit;

namespace PicoFx.D2 {
	[StructLayout( LayoutKind.Explicit, Size = 128 )]
	public struct Position {
		[FieldOffset( 56 )]
		public readonly long Working;
		[FieldOffset( 120 )]
		public readonly long Committed;

		public Position( long working, long committed ) {
			Working = working;
			Committed = committed;
		}

		internal Position CatchUpWith( long newWorking ) {
			return new Position( newWorking, Committed );
		}

		internal Position Commit() {
			return new Position( Working, Working );
		}
	}

	public class Writer<T> : IObserver<T>, IDisposable {
		private readonly D2<T> _disruptor;
		public Position Position = new Position( -1, -1 );

		public Writer( D2<T> disruptor ) {
			_disruptor = disruptor;
			_disruptor.RegisterProducer( this );
		}

		public void OnNext( T value ) {
			_disruptor.Add( value, this );
		}

		public void OnError( Exception error ) {
			throw error; // Handle more gracefully
		}

		public void OnCompleted() {
			_disruptor.UnregisterProducer( this );
		}

		public void CatchUpWith( long newWorking ) {
			Position = Position.CatchUpWith( newWorking );
		}

		public void Commit() {
			_disruptor.CommitProduct( this );
			Position = Position.Commit();
		}

		public void Dispose() {
			OnCompleted();
		}
	}

	public class Reader<T> : IDisposable {
		private readonly D2<T> _disruptor;
		public Position Position = new Position( -1, -1 );

		public Reader( D2<T> disruptor ) {
			_disruptor = disruptor;
			_disruptor.RegisterConsumer( this );
		}

		public IEnumerable<T> ConsumeBatch() {
			return _disruptor.WaitForBatch( this );
		}

		public void Dispose() {
			_disruptor.UnregisterConsumer( this );
		}

		public void CatchUpWith( long newWorking ) {
			Position = Position.CatchUpWith( newWorking );
		}

		public void Commit() {
			Position = Position.Commit();
			_disruptor.CommitConsumption();
		}
	}

	public class SimpleImmutableList<T> : IEnumerable<T> {
		private readonly T[] _array = new T[ 0 ];

		public SimpleImmutableList() { }

		public SimpleImmutableList( T[] array ) {
			_array = array;
		}

		public SimpleImmutableList<T> Add( T t ) {
			return new SimpleImmutableList<T>( _array.Union( new[] { t } ).ToArray() );
		}

		public SimpleImmutableList<T> Remove( T t ) {
			return new SimpleImmutableList<T>( _array.Except( new[] { t } ).ToArray() );
		}

		public int Count { get { return _array.Length; } }

		public IEnumerator<T> GetEnumerator() {
			return _array.AsEnumerable().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return _array.GetEnumerator();
		}
	}

	public class D2<T> : IDisposable {
		private readonly RingBuffer<T> _buffer;

		public D2( int size ) {
			_buffer = new RingBuffer<T>( size );
		}

		public D2( RingBuffer<T> buffer ) {
			_buffer = buffer;
		}

		private long _headWorking = -1, _headCommitted = -1;
		private long _tailCommitted = -1;
		private SimpleImmutableList<Writer<T>> _producers = new SimpleImmutableList<Writer<T>>();
		private SimpleImmutableList<Reader<T>> _consumers = new SimpleImmutableList<Reader<T>>();

		public void Add( T value, Writer<T> writer ) {
			if ( _producerCount > 1 )
				MultiThreadedAdd( value, writer );
			else
				SingleThreadedAdd( value, writer );
		}

		private void SingleThreadedAdd( T value, Writer<T> writer ) {
			while ( !ReadyToAdd( writer ) ) {
				if ( !Thread.Yield() ) {
					Thread.Sleep( 1 );
				}
			}

			_buffer[ ++_headWorking ] = value;
			writer.CatchUpWith( _headWorking );
		}

		private void MultiThreadedAdd( T value, Writer<T> writer ) {
			long original, proposed;
			do {
				do {
					original = _headWorking;
					proposed = original + 1;

					while(!ReadyToAdd(writer))
						if(!Thread.Yield())
							Thread.Sleep(1);
				} while(original != _headWorking);

			} while ( original != Interlocked.CompareExchange( ref _headWorking, proposed, original ) );
			_buffer[ proposed ] = value;
			writer.CatchUpWith( proposed );
		}

		private bool ReadyToAdd( Writer<T> writer ) {
			return ( _headWorking == _headCommitted || _headWorking == writer.Position.Working )
				&& !( _headWorking - _tailCommitted >= _buffer.Size - 1 );
		}

		public void CommitProduct( Writer<T> writer ) {
			if ( _headWorking != writer.Position.Working )
				throw new InvalidOperationException( "The thread that's trying to commit does not own the head." );

			_headCommitted = _headWorking;
		}

		public IEnumerable<T> WaitForBatch( Reader<T> reader ) {
			Contract.Ensures( Contract.Result<IEnumerable<T>>() != null );

			while ( _headCommitted == reader.Position.Working && !_aborting )
				if ( !Thread.Yield() )
					Thread.Sleep( 1 );

			long catchUpTo = _headCommitted;
			if ( catchUpTo > reader.Position.Working ) {
				long start = reader.Position.Working + 1;
				reader.CatchUpWith( catchUpTo );
				return _buffer.Slice( start, catchUpTo );
			}

			return Enumerable.Empty<T>(); // Signal end of stream
		}

		public void CommitConsumption() {
			_tailCommitted = _consumers.Min( c => c.Position.Committed );
		}

		private bool _aborting;
		private int _producerCount;

		public void RegisterProducer( Writer<T> writer ) {
			SimpleImmutableList<Writer<T>> original, proposed;
			do {
				original = _producers;
				proposed = original.Add( writer );
			}
			while ( original != Interlocked.CompareExchange( ref _producers, proposed, original ) );
			_producerCount = _producers.Count;
		}

		public void UnregisterProducer( Writer<T> writer ) {
			SimpleImmutableList<Writer<T>> original, proposed;
			do {
				original = _producers;
				proposed = original.Remove( writer );
			}
			while ( original != Interlocked.CompareExchange( ref _producers, proposed, original ) );
			_producerCount = _producers.Count;
		}

		public void RegisterConsumer( Reader<T> reader ) {
			SimpleImmutableList<Reader<T>> original, proposed;
			do {
				original = _consumers;
				proposed = original.Add( reader );
			}
			while ( original != Interlocked.CompareExchange( ref _consumers, proposed, original ) );
		}

		public void UnregisterConsumer( Reader<T> reader ) {
			SimpleImmutableList<Reader<T>> original, proposed;
			do {
				original = _consumers;
				proposed = original.Remove( reader );
			}
			while ( original != Interlocked.CompareExchange( ref _consumers, proposed, original ) );
		}

		public void Dispose() {
			_aborting = true;
		}
	}

	public class DTTests {

		[Fact]
		public void D2_Can_Add_Values() {
			// Arrange
			var buffer = new TracingRingBuffer<int>( 16 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 1 ];

			// Act
			var consumer = new Thread( () => {
				using ( var reader = new Reader<int>( sut ) ) {
					IEnumerable<int> batch;
					do {
						batch = reader.ConsumeBatch();
						count[ 0 ] += batch.Count();
						reader.Commit();
					} while ( batch.Any() );
				}
			} );
			consumer.Start();

			using ( var writer = new Writer<int>( sut ) ) {
				writer.OnNext( 12 );
				writer.OnNext( 13 );
				writer.OnNext( 14 );
				writer.OnNext( 15 );
				writer.Commit();
				writer.OnNext( 16 );
				writer.OnNext( 17 );
				writer.Commit();

				Thread.Sleep( 10 );
			}

			sut.Dispose();
			consumer.Join();

			// Assert
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( 6, count[ 0 ] );
		}

		[Fact]
		public void Two_D2s_Can_Add_Values() {
			// Arrange
			var buffer = new TracingRingBuffer<int>( 16 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 1 ];

			// Act
			var consumer = new Thread( () => {
				using ( var reader = new Reader<int>( sut ) ) {
					IEnumerable<int> batch;
					do {
						batch = reader.ConsumeBatch();
						count[ 0 ] += batch.Count();
						reader.Commit();
					} while ( batch.Any() );
				}
			} );
			consumer.Start();

			var producer1 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					writer.OnNext( 20 );
					writer.Commit();
					Thread.Sleep( 20 );
					writer.OnNext( 21 );
					writer.Commit();
				}
			} );
			producer1.Start();

			using ( var writer = new Writer<int>( sut ) ) {
				writer.OnNext( 1 );

				Thread.Sleep( 20 );
				writer.OnNext( 2 );

				Thread.Sleep( 20 );
				writer.OnNext( 3 );

				writer.Commit();
			}

			Thread.Sleep( 10 );

			producer1.Join();
			sut.Dispose();
			consumer.Join();

			// Assert
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( 5, count[ 0 ] );
		}


		[Fact]
		public void Performance_is_above_3_million_TPS_when_two_consumers_and_four_producers() {

			// Arrange
			const int itemCount = 500000;
			var buffer = new TracingRingBuffer<int>( 10000 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 2 ];

			// Act
			new Thread( () => {
				using ( var reader = new Reader<int>( sut ) ) {
					IEnumerable<int> batch;
					do {
						batch = reader.ConsumeBatch();
						count[ 0 ] += batch.Count();
						reader.Commit();
					} while ( batch.Any() );
				}
			} ).Start();

			new Thread( () => {
				using ( var reader = new Reader<int>( sut ) ) {
					IEnumerable<int> batch;
					do {
						batch = reader.ConsumeBatch();
						count[ 1 ] += batch.Count();
						reader.Commit();
					} while ( batch.Any() );
				}
			} ).Start();

			const int itemsPerThread = itemCount / 4;
			var producer1 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					for ( int i = 0; i < itemsPerThread; i++ ) {
						writer.OnNext( i );
						if ( i % 30 == 0 )
							writer.Commit();
					}
					writer.Commit();
				}
			} );

			var producer2 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					for ( int i = 0; i < itemsPerThread; i++ ) {
						writer.OnNext( i );
						if ( i % 10 == 0 )
							writer.Commit();
					}
					writer.Commit();
				}
			} );

			var producer3 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					for ( int i = 0; i < itemsPerThread; i++ ) {
						writer.OnNext( i );
						if ( i % 10 == 0 )
							writer.Commit();
					}
					writer.Commit();
				}
			} );

			var producer4 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					for ( int i = 0; i < itemsPerThread; i++ ) {
						writer.OnNext( i );
						if ( i % 10 == 0 )
							writer.Commit();
					}
					writer.Commit();
				}
			} );

			var timer = new Stopwatch();
			timer.Start();

			producer1.Start();
			producer2.Start();
			producer3.Start();
			producer4.Start();

			producer1.Join();
			producer2.Join();
			producer3.Join();
			producer4.Join();

			sut.Dispose();
			timer.Stop();

			// Assert

			Assert.Equal( itemCount, buffer.GetFilledPositionsCount() );
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( itemCount, count[ 0 ] );
			Assert.Equal( itemCount, count[ 1 ] );
			double tps = itemCount / timer.Elapsed.TotalSeconds;
			Console.WriteLine( string.Format( "TPS ~ {0:0.00} million", tps / 1000000 ) );
			Assert.True( tps > 3000000 );
		}
		[Fact]
		public void Performance_is_above_3_million_TPS_for_single_producer() {

			// Arrange
			const int itemCount = 500000;
			var buffer = new TracingRingBuffer<int>( 10000 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 1 ];

			// Act
			new Thread( () => {
				using ( var reader = new Reader<int>( sut ) ) {
					IEnumerable<int> batch;
					do {
						batch = reader.ConsumeBatch();
						count[ 0 ] += batch.Count();
						reader.Commit();
					} while ( batch.Any() );
				}
			} ).Start();

			var producer1 = new Thread( () => {
				using ( var writer = new Writer<int>( sut ) ) {
					for ( int i = 0; i < itemCount; i++ ) {
						writer.OnNext( i );
						if ( i % 1000 == 0 )
							writer.Commit();
					}
					writer.Commit();
				}
			} );

			var timer = new Stopwatch();
			timer.Start();

			producer1.Start();
			producer1.Join();

			sut.Dispose();
			timer.Stop();

			// Assert

			Assert.Equal( itemCount, buffer.GetFilledPositionsCount() );
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( itemCount, count[ 0 ] );
			double tps = itemCount / timer.Elapsed.TotalSeconds;
			Console.WriteLine( string.Format( "TPS ~ {0:0.00} million", tps / 1000000 ) );
			Assert.True( tps > 3500000 );
		}
	}
}
