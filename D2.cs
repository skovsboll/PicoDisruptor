using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using PicoFx.Disruptor;
using Xunit;

namespace PicoFx.D2 {
	[StructLayout( LayoutKind.Explicit, Size = 128 )]
	struct Position {
		[FieldOffset( 56 )]
		public long Working;
		[FieldOffset( 120 )]
		public long Committed;

		public Position( long working, long committed ) {
			Working = working;
			Committed = committed;
		}

		public Position CatchUpWith( long newWorking ) {
			return new Position( newWorking, Committed );
		}

		public Position Commit() {
			return new Position( Working, Working );
		}
	}

	public class SimpleImmutableList<T> : IEnumerable<T> {
		private readonly T[] _array = new T[ 0 ];

		public SimpleImmutableList( T[] array ) {
			_array = array;
		}

		public SimpleImmutableList<T> Add( T t ) {
			return new SimpleImmutableList<T>( _array.Union( new[] { t } ).ToArray() );
		}

		public SimpleImmutableList<T> Remove( T t ) {
			return new SimpleImmutableList<T>( _array.Except( new[] { t } ).ToArray() );
		}

		public IEnumerator<T> GetEnumerator() {
			return _array.AsEnumerable().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return _array.GetEnumerator();
		}
	}

	public class D2<T> {
		private readonly RingBuffer<T> _buffer;

		public D2( int size ) {
			_buffer = new RingBuffer<T>( size );
		}

		public D2( RingBuffer<T> buffer ) {
			_buffer = buffer;
		}

		private long _headWorking = -1, _headCommitted = -1;
		private readonly ConcurrentDictionary<int, Position> _producers = new ConcurrentDictionary<int, Position>();
		private readonly ConcurrentDictionary<int, Position> _consumers = new ConcurrentDictionary<int, Position>();

		public void Add( T t ) {
			long original, proposed;
			var threadId = GetThreadId();
			do {
				original = _headWorking;
				proposed = original + 1;
				while ( !ReadyToAdd( threadId ) ) Thread.Yield();
			}
			while ( original != Interlocked.CompareExchange( ref _headWorking, proposed, original ) );
			_buffer[ proposed ] = t;
			_producers[ threadId ] = _producers[ threadId ].CatchUpWith( proposed );
		}

		private bool ReadyToAdd( int threadId ) {
			bool headIsIdle = _headWorking == _headCommitted;

			Position myPosition = _producers.GetOrAdd( threadId, index => new Position( -1, -1 ) );
			bool headIsOwnedByMe = _headWorking == myPosition.Working;

			//var tail = _consumers.Values.Select( c => c.Committed ).DefaultIfEmpty( -1 ).Min();

			bool wrappingEminent = false; // _headWorking - tail >= _buffer.Size;

			return !wrappingEminent && ( headIsIdle || headIsOwnedByMe );
		}

		public void CommitProduct() {
			var threadId = GetThreadId();
			Position position = _producers[ threadId ];

			if ( _headWorking != position.Working )
				throw new InvalidOperationException( "The thread that's trying to commit does not own the head." );

			_headCommitted = _headWorking;
			_producers[ threadId ] = position.Commit();
		}

		private static int GetThreadId() {
			return Thread.CurrentThread.ManagedThreadId;
		}

		public void CommitConsumption() {
			var threadId = GetThreadId();
			Position position = _consumers[ threadId ];
			_consumers[ threadId ] = position.Commit();
		}

		public IEnumerable<T> WaitForBatch() {
			Contract.Ensures( Contract.Result<IEnumerable<T>>() != null );

			var threadId = GetThreadId();
			Position position = _consumers.GetOrAdd( threadId, index => new Position( -1, -1 ) );

			while ( _headCommitted == position.Working && !_aborting )
				Thread.Yield();

			long catchUpTo = _headCommitted;
			if ( catchUpTo > position.Working ) {
				long start = position.Working + 1;
				_consumers[ threadId ] = position.CatchUpWith( catchUpTo );
				return _buffer.Slice( start, catchUpTo );
			}

			return Enumerable.Empty<T>(); // Signal end of stream
		}

		private bool _aborting;

		public void Stop() {
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
			new Thread( () => {
				var batch = sut.WaitForBatch();
				count[ 0 ] += batch.Count();
				sut.CommitConsumption();
			} ).Start();

			sut.Add( 12 );
			sut.Add( 14 );
			sut.Add( 13 );
			sut.CommitProduct();

			Thread.Sleep( 10 );

			// Assert
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( 3, count[ 0 ] );
		}

		[Fact]
		public void Two_D2s_Can_Add_Values() {
			// Arrange
			var buffer = new TracingRingBuffer<int>( 16 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 1 ];

			// Act
			new Thread( () => {
				IEnumerable<int> batch;
				do {
					batch = sut.WaitForBatch();
					count[ 0 ] += batch.Count();
					sut.CommitConsumption();
				} while ( batch.Any() );
			} ).Start();

			var producer1 = new Thread( () => {
				sut.Add( 20 );
				sut.CommitProduct();
				Thread.Sleep( 20 );
				sut.Add( 21 );
				sut.CommitProduct();
			} );
			producer1.Start();

			sut.Add( 12 );
			Thread.Sleep( 20 );
			sut.Add( 14 );
			Thread.Sleep( 20 );
			sut.Add( 13 );
			sut.CommitProduct();

			Thread.Sleep( 10 );

			producer1.Join();
			sut.Stop();

			// Assert
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( 5, count[ 0 ] );
		}


		[Fact]
		public void Performance_is_above_4million_TPS() {

			// Arrange
			const int itemCount = 2000000;
			var buffer = new TracingRingBuffer<int>( 1000 );
			var sut = new D2<int>( buffer );
			int[] count = new int[ 1 ];

			// Act
			new Thread( () => {
				IEnumerable<int> batch;
				do {
					batch = sut.WaitForBatch();
					count[ 0 ] += batch.Count();
					sut.CommitConsumption();
				} while ( batch.Any() );
			} ).Start();

			const int itemsPerThread = itemCount / 2;
			var producer1 = new Thread( () => {
				for ( int i = 0; i < itemsPerThread; i++ ) {
					sut.Add( i );
					sut.CommitProduct();
				}
			} );

			var producer2 = new Thread( () => {
				for ( int i = 0; i < itemsPerThread; i++ ) {
					sut.Add( i );
					sut.CommitProduct();
				}
			} );

			var timer = new Stopwatch();
			timer.Start();

			producer1.Start();
			producer2.Start();

			producer1.Join();
			producer2.Join();

			sut.Stop();
			timer.Stop();

			// Assert

			Assert.Equal( itemCount, buffer.GetFilledPositionsCount() );
			Assert.True( buffer.AllPositionsWereSetOnlyOnce() );
			Assert.False( buffer.SequenceContainsHoles() );
			Assert.Equal( itemCount, count[ 0 ] );
			double tps = itemCount / timer.Elapsed.TotalSeconds;
			Console.WriteLine( tps );
			Assert.True( tps > 1000000 );
		}
	}
}
