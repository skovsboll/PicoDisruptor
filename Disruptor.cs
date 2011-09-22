using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.InteropServices;
using System.Threading;
using Xunit.Sdk;

namespace PicoFx.Disruptor {

	/// <summary>
	/// RINGBUFFER
	/// </summary>

	public class RingBuffer<T> {
		private readonly ulong _sizeMinusOne;
		private readonly T[] _slots;

		public RingBuffer( int size ) {
			size--;
			ulong powerOf2Size = 1;
			while ( size > 0 ) {
				size = size >> 1;
				powerOf2Size = powerOf2Size << 1;
			}

			_slots = new T[ powerOf2Size ];
			_sizeMinusOne = powerOf2Size - 1;
		}

		public int Size {
			get { return _slots.Length; }
		}

		public virtual T this[ long index ] {
			get { return _slots[ IndexToSlot( index ) ]; }
			set {
				_slots[ IndexToSlot( index ) ] = value;
			}
		}

		public IEnumerable<T> Slice( long start, long end ) {
			for ( long i = start; i <= end; i++ )
				yield return this[ i ];
		}

		private long IndexToSlot( long index ) {
			return (long)( (ulong)index & _sizeMinusOne );
		}
	}

	/// <summary>
	/// WAITSTRATEGY
	/// </summary>

	public interface WaitStrategy {
		void WaitFor( Func<bool> condition );
	}

	public class ThreadYieldWaitStrategy : WaitStrategy {
		public void WaitFor( Func<bool> condition ) {
			while ( !condition() ) {
				bool otherThreadsAvailable = Thread.Yield(); // Yield to other processes on same CPU
				if ( !otherThreadsAvailable ) {
					Thread.Sleep( 1 ); // Yield to any CPU
				}
			}
		}
	}

	/// <summary>
	/// DISRUPTOR
	/// </summary>

	public class Disruptor<T> : IObservable<IEnumerable<T>>, IDisposable {
		private readonly RingBuffer<T> _buffer;

		private readonly HeadOfBuffer _headOfBuffer;
		private readonly ConcurrentDictionary<int, ConsumingWorker> _consumers = new ConcurrentDictionary<int, ConsumingWorker>();
		private readonly ConcurrentDictionary<int, ProducingWorker> _producers = new ConcurrentDictionary<int, ProducingWorker>();

		private readonly WaitStrategy _waitStrategy;

		public Disruptor( int size ) : this( size, new ThreadYieldWaitStrategy() ) { }

		public Disruptor( int size, WaitStrategy waitStrategy ) {
			_waitStrategy = waitStrategy;
			_buffer = new RingBuffer<T>( size );
			_headOfBuffer = new HeadOfBuffer( _buffer.Size );
		}

		public Disruptor( WaitStrategy waitStrategy, RingBuffer<T> ringBuffer ) {
			_waitStrategy = waitStrategy;
			_buffer = ringBuffer;
			_headOfBuffer = new HeadOfBuffer( _buffer.Size );
		}

		public void Put( T value ) {
			ProducingWorker producer = _producers[ Thread.CurrentThread.ManagedThreadId ];
			producer.AcquireNext();
			_buffer[ producer.Position.WorkingPosition ] = value;
		}

		public void Put( IEnumerable<T> values ) {
			ProducingWorker producer = _producers[ Thread.CurrentThread.ManagedThreadId ];
			foreach ( T value in values ) {
				producer.AcquireNext();
				_buffer[ producer.Position.WorkingPosition ] = value;
			}
		}

		internal IEnumerable<T> ConsumeNextBatch() {
			ConsumingWorker consumer = _consumers[ Thread.CurrentThread.ManagedThreadId ];

			_waitStrategy.WaitFor( () => ReadyToConsume( consumer ) || !_producers.Any() );

			if ( ReadyToConsume( consumer ) ) {
				consumer.CatchUpWith( _headOfBuffer.Position.CommittedPosition );
				return _buffer.Slice( consumer.Position.CommittedPosition + 1, consumer.Position.WorkingPosition );
			}

			return null;
		}

		public void Commit() {
			Worker worker = GetCurrentWorker();
			worker.Commit();
		}

		private bool ReadyToConsume( Worker consumer ) {
			return _headOfBuffer.Position.CommittedPosition > consumer.Position.WorkingPosition;
		}

		private Worker GetCurrentWorker() {
			int threadId = Thread.CurrentThread.ManagedThreadId;

			if ( _producers.ContainsKey( threadId ) ) {
				return _producers[ threadId ];
			}

			if ( _consumers.ContainsKey( threadId ) ) {
				return _consumers[ threadId ];
			}

			throw new IndexOutOfRangeException( "Unknown worker" );
		}

		[ContractInvariantMethod]
		private void ClassInvariants() {
			//Contract.Invariant( !_cleaners.Any() || _producers.Values.All( p => FindTail().CommittedPosition - p.CommittedPosition < _buffer.Size ), "A producer is exceeding the ringbuffer capacity." );
			//Contract.Invariant( _producers.ToArray().Count( p => p.Value.WorkingPosition > p.Value.CommittedPosition ) <= 1, "Only one producer can be producing at any given time." );
			Contract.Invariant( _producers.Keys.Union( _consumers.Keys ).Distinct().Count() == _producers.Count + _consumers.Count, "The same thread registered in multiple roles." );
		}

		public IDisposable Subscribe( IObserver<IEnumerable<T>> batchObserver ) {
			return new DisruptorSubscription<T>( batchObserver, this, Role.Consumer );
		}

		public IDisposable RegisterThread( Role role ) {
			int threadId = Thread.CurrentThread.ManagedThreadId;
			switch ( role ) {
				case Role.Producer:
					var threadSafeProducer = new ThreadSafeProducer<T>( _waitStrategy, _headOfBuffer, FindTail );
					_producers.TryAdd( threadId, threadSafeProducer );
					return Disposable.Create( () => {
						ProducingWorker worker;
						if ( !_producers.TryRemove( threadId, out worker ) )
							throw new DoesNotContainException( threadSafeProducer );
					} );

				case Role.Consumer:
					var consumer = new Consumer();
					_consumers.TryAdd( threadId, consumer );
					return Disposable.Create( () => {
						ConsumingWorker worker;
						if ( !_consumers.TryRemove( threadId, out worker ) )
							throw new DoesNotContainException( consumer );
					} );

				default:
					throw new ArgumentOutOfRangeException( "Unknown role " + role );
			}
		}

		public void Dispose() {
			_producers.Clear();
			// Give consumers a chance to consume the last items.
		}

		private long FindTail() {
			return _consumers.Values.Select( w => w.Position.CommittedPosition ).DefaultIfEmpty( -1 ).Min();
		}
	}

	/// <summary>
	/// SUBSCRIPTION
	/// </summary>

	public class DisruptorSubscription<T> : IDisposable {
		private bool _stopping;

		public DisruptorSubscription( IObserver<IEnumerable<T>> observer, Disruptor<T> disruptor, Role role ) {
			using ( disruptor.RegisterThread( role ) ) {
				while ( !_stopping ) {
					try {
						IEnumerable<T> batch = disruptor.ConsumeNextBatch();
						if ( batch != null )
							observer.OnNext( batch );
						else
							_stopping = true;
					}
					catch ( Exception exception ) {
						observer.OnError( exception );
					}
					finally {
						disruptor.Commit();
					}
				}
				observer.OnCompleted();
			}
		}
		public void Dispose() {
			_stopping = true;
		}
	}


	public enum Role {
		Producer,
		Consumer,
	}

	public interface Worker {
		CachePaddedPosition Position { get; }
		void Commit();
	}

	public interface ConsumingWorker : Worker {
		void CatchUpWith( long committedPosition );
	}

	public interface ProducingWorker : Worker {
		void AcquireNext();
	}

	[StructLayout( LayoutKind.Explicit, Size = 128 )]
	public struct CachePaddedPosition {
		[FieldOffset( 56 )]
		public long WorkingPosition;

		[FieldOffset( 120 )]
		public long CommittedPosition;
	}

	public class HeadOfBuffer {
		public HeadOfBuffer( int bufferSize ) {
			BufferSize = bufferSize;
			Position.CommittedPosition = Position.WorkingPosition = -1;
		}
		public CachePaddedPosition Position;
		public readonly int BufferSize;
	}

	/// <summary>
	/// CONSUMER
	/// </summary>

	public class Consumer : ConsumingWorker {
		private CachePaddedPosition _position;

		public Consumer() {
			_position.CommittedPosition = _position.WorkingPosition = -1;
		}

		public CachePaddedPosition Position {
			get { return _position; }
		}

		public void Commit() {
			_position.CommittedPosition = _position.WorkingPosition;
		}

		public void CatchUpWith( long committedPosition ) {
			_position.WorkingPosition = committedPosition;
		}
	}


	/// <summary>
	/// PRODUCER
	/// </summary>

	public class ThreadSafeProducer<T> : ProducingWorker {
		private readonly HeadOfBuffer _head;
		private readonly WaitStrategy _waitStrategy;
		private readonly Func<long> _findTail;
		private CachePaddedPosition _position;

		public ThreadSafeProducer( WaitStrategy waitStrategy, HeadOfBuffer head, Func<long> findTail ) {
			_findTail = findTail;
			_head = head;
			_waitStrategy = waitStrategy;
			_position.WorkingPosition = _position.CommittedPosition = -1;
		}

		public CachePaddedPosition Position {
			get { return _position; }
		}

		public void AcquireNext() {
			long original, changed;
			do {
				_waitStrategy.WaitFor( ReadyToAcquire );
				original = _head.Position.WorkingPosition;
				changed = original + 1;
			} while ( original != Interlocked.CompareExchange( ref _head.Position.WorkingPosition, changed, original ) );
			_position.WorkingPosition = changed;
		}

		public void Commit() {
			_position.CommittedPosition = _position.WorkingPosition;
			_head.Position.CommittedPosition = _position.CommittedPosition;
		}

		private bool ReadyToAcquire() {
			bool headIsFree = _head.Position.WorkingPosition == _head.Position.CommittedPosition;
			bool iOwnTheHead = _head.Position.WorkingPosition == _position.WorkingPosition;
			long tail = _findTail();
			bool noWrappingWillOccur = _head.Position.WorkingPosition - tail < _head.BufferSize;
			return noWrappingWillOccur && ( headIsFree || iOwnTheHead );
		}
	}
}