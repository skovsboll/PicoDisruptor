using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using PicoFx.Disruptor;
using PicoIoc;
using Xunit;

namespace PicoFx.D3 {

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

		public bool Idle() {
			return Working == Committed;
		}
	}

	class Head {
		public Position Position = new Position( -1, -1 );
	}

	class Writer<T> : IObserver<T>, IDisposable {
		private readonly D3<T> _disruptor;

		public readonly Head Current = new Head();

		public Writer( D3<T> disruptor ) {
			_disruptor = disruptor;
		}

		public void OnNext( T value ) {
			_disruptor.OnNext(value, this);
		}

		public void OnError( Exception error ) {

		}

		public void OnCompleted() {
			_disruptor.OnCompleted( this );
		}

		public void Dispose() {
			OnCompleted();
		}
	}

	class D3<T> : IObservable<T>, IDisposable {
		private ConcurrentBag<IObserver<T>> _observers = new ConcurrentBag<IObserver<T>>();
		private readonly ConcurrentBag<Writer<T>> _writers = new ConcurrentBag<Writer<T>>();

		private readonly RingBuffer<T> _buffer;
		private readonly Head _head = new Head();

		public D3( RingBuffer<T> buffer ) {
			_buffer = buffer;
		}

		internal long OnNext( T value, Writer<T> writer ) {
			long original, proposed;
			do {
				while ( !ReadyToRockAndRoll( writer ) )
					if ( !Thread.Yield() ) Thread.Sleep( 1 );

				original = _head.Position.Working;
				proposed = original + 1;
			}
			while ( original != Interlocked.CompareExchange( ref _head.Position.Working, proposed, original ) );
			_buffer[ proposed ] = value;
			writer.Current.Position = writer.Current.Position.CatchUpWith( proposed );
			_head.Position = _head.Position.Commit();
			writer.Current.Position = writer.Current.Position.Commit();

			_observers.Each(o => o.OnNext(value));

			return proposed;
		}

		private bool ReadyToRockAndRoll( Writer<T> writer ) {
			return _head.Position.Idle() || _head.Position.Working == writer.Current.Position.Working;
		}

		public IDisposable Subscribe( IObserver<T> observer ) {
			Contract.Requires( observer != null );
			_observers.Add( observer );
			return new Subscription<T>( observer, this );
		}

		internal class Subscription<T1> : IDisposable {
			private readonly D3<T> _disruptor;
			private readonly IObserver<T> _observer;

			public Subscription( IObserver<T> observer, D3<T> disruptor ) {
				Contract.Requires( observer != null );
				Contract.Requires( disruptor != null );
				_observer = observer;
				_disruptor = disruptor;
			}

			public void Dispose() {
				_disruptor.Unsubscribe( _observer );
			}
		}

		private void Unsubscribe( IObserver<T> observer ) {
			Contract.Requires( observer != null );

			while ( !_observers.TryTake( out observer ) ) {
				Thread.Yield();
			}
		}

		public void OnCompleted( Writer<T> writer ) {
			Contract.Requires( writer != null );

			while ( _writers.TryTake( out writer ) )
				Thread.Yield();
		}

		public void Dispose() {
			_observers.Each( o => o.OnCompleted() );
			_observers = new ConcurrentBag<IObserver<T>>();
		}
	}

	public class SubPubTests {
		[Fact]
		public void SubjectCanDistributeMessagesFast() {
			int[] count = new int[ 4 ];

			// Arrange
			var sut = new D3<int>( new RingBuffer<int>( 1024 ) );

			using ( var sub1 = sut.Subscribe( i => count[ 0 ]++ ) )
			using ( var sub2 = sut.Subscribe( i => count[ 1 ]++ ) )
			using ( var sub3 = sut.Subscribe( i => count[ 2 ]++ ) )
			using ( var sub4 = sut.Subscribe( i => count[ 3 ]++ ) ) {

				// Act
				var threads = new Thread[ 10 ];

				foreach ( int i in Enumerable.Range( 0, 10 ) ) {
					var thread = new Thread( () => {
						using ( var w = new Writer<int>( sut ) ) {
							foreach ( int j in Enumerable.Range( 0, 100000 ) )
								w.OnNext( j );

							w.OnCompleted();
						}
					} );
					threads[ i ] = thread;
				}

				var stopwatch = new Stopwatch();
				stopwatch.Start();
				threads.Each( t => t.Start() );
				threads.Each( t => t.Join() );
				stopwatch.Stop();

				Console.WriteLine( 1000000 / stopwatch.Elapsed.TotalSeconds );

			}

			// Assert
			Assert.Equal( 1000000, count[ 0 ] );
			Assert.Equal( 1000000, count[ 1 ] );
			Assert.Equal( 1000000, count[ 2 ] );
			Assert.Equal( 1000000, count[ 3 ] );
		}


	}
}
