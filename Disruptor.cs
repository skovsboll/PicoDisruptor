using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reactive.Disposables;
using System.Runtime.InteropServices;
using System.Threading;

namespace PicoFx.Disruptor {

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

		private long IndexToSlot( long index ) {
			return (long)( (ulong)index & _sizeMinusOne );
		}

		public IEnumerable<T> Slice( long start, long end ) {
			for ( long i = start; i <= end; i++ ) {
				yield return this[ i ];
			}
		}
	}

	public interface WaitStrategy {
		void WaitFor( Func<bool> condition );
	}

	public class ThreadYieldWaitStrategy : WaitStrategy {
		#region WaitStrategy Members

		public void WaitFor( Func<bool> condition ) {
			while ( !condition() ) {
				bool otherThreadsAvailable = Thread.Yield(); // Yield to other processes on same CPU
				if ( !otherThreadsAvailable ) {
					Thread.Sleep( 1 ); // Yield to any CPU
				}
			}
		}

		#endregion
	}

	public class Disruptor<T> {
		private readonly RingBuffer<T> _buffer;

		private readonly IDictionary<int, ConsumingWorker> _consumers = new Dictionary<int, ConsumingWorker>();
		private readonly BufferRange _bufferRange;
		private readonly IDictionary<int, ProducingWorker> _producers = new Dictionary<int, ProducingWorker>();

		private readonly WaitStrategy _waitStrategy;
		private bool _aborting;
		private ConsumingWorker _cleaner;
		private int _cleanerManagedId;
		static object Door = new object();

		public Disruptor( int size ) : this( size, new ThreadYieldWaitStrategy() ) { }

		public Disruptor( int size, WaitStrategy waitStrategy ) {
			_waitStrategy = waitStrategy;
			_buffer = new RingBuffer<T>( size );
			_bufferRange = new BufferRange( _buffer.Size );
		}

		public Disruptor( WaitStrategy waitStrategy, RingBuffer<T> ringBuffer ) {
			_waitStrategy = waitStrategy;
			_buffer = ringBuffer;
			_bufferRange = new BufferRange( _buffer.Size );
		}

		public IObservable<T> Cleaning {
			get { return new ConsumerHotObservable<T>( CleanNextBatch, Commit ); }
		}

		public IObservable<T> Consuming {
			get { return new ConsumerHotObservable<T>( ConsumeNextBatch, Commit ); }
		}

		private bool AllPartiesRegistered {
			get { return _cleaner != null && _producers.Count > 0; }
		}

		public void Put( T value ) {
			ProducingWorker producer = RegisterProducer();

			_waitStrategy.WaitFor( () => _aborting || ( AllPartiesRegistered ) );

			if ( _aborting ) {
				return;
			}

			producer.AcquireNext();

			_buffer[ producer.WorkingPosition ] = value;
		}

		public void Put( IEnumerable<T> values ) {
			ProducingWorker producer = RegisterProducer();
			_waitStrategy.WaitFor( () => AllPartiesRegistered );

			foreach ( T value in values ) {
				producer.AcquireNext();
				_buffer[ producer.WorkingPosition ] = value;
			}
		}

		public IEnumerable<T> ConsumeNextBatch() {
			Contract.Ensures( Contract.Result<IEnumerable<T>>() != null );

			ConsumingWorker consumer = RegisterConsumer();
			_waitStrategy.WaitFor( () => AllPartiesRegistered && ReadyToConsume( consumer ) );

			consumer.CatchUpWith( _producers.Values.OrderByDescending( p => p.CommittedPosition ).First() );

			return _buffer.Slice( consumer.CommittedPosition + 1, consumer.WorkingPosition );
		}

		public IEnumerable<T> CleanNextBatch() {
			Contract.Ensures( Contract.Result<IEnumerable<T>>() != null );

			ConsumingWorker cleaner = RegisterCleaner();
			_waitStrategy.WaitFor( () => AllPartiesRegistered && ReadyToClean() );

			cleaner.CatchUpWith( _consumers.Any()
							? (Worker)_consumers.Values.OrderBy( c => c.CommittedPosition ).First()
							: _producers.Values.OrderByDescending( c => c.CommittedPosition ).First() );

			return _buffer.Slice( cleaner.CommittedPosition + 1, cleaner.WorkingPosition );
		}

		public void Commit() {
			Worker worker = GetCurrentWorker();
			worker.Commit();
		}

		public void Stop() {
			_aborting = true;
		}

		//private bool ReadyToProduce() {
		//      return _producers.Values.All( p => p.WorkingPosition - _cleaner.CommittedPosition < _buffer.Size );
		//}

		private bool ReadyToConsume( Worker consumer ) {
			return _producers.Values.Any( p => p.CommittedPosition > consumer.WorkingPosition );
		}

		private bool ReadyToClean() {
			if ( _consumers.Count > 0 ) {
				return _consumers.Values.All( r => r.CommittedPosition > _cleaner.WorkingPosition );
			}
			else {
				return _producers.Values.Any( p => p.CommittedPosition > _cleaner.WorkingPosition );
			}
		}

		private Worker GetCurrentWorker() {
			int threadId = Thread.CurrentThread.ManagedThreadId;
			if ( _cleanerManagedId == threadId ) {
				return _cleaner;
			}

			if ( _producers.ContainsKey( threadId ) ) {
				return _producers[ threadId ];
			}

			if ( _consumers.ContainsKey( threadId ) ) {
				return _consumers[ threadId ];
			}

			throw new IndexOutOfRangeException( "Unknown worker" );
		}

		private ProducingWorker RegisterProducer() {
			int threadId = Thread.CurrentThread.ManagedThreadId;
			if ( !_producers.ContainsKey( threadId ) ) {
				var producer = new ThreadSafeProducer( _waitStrategy, _bufferRange );
				lock ( Door ) {
					_producers.Add( threadId, producer );
				}
				return producer;
			}

			return _producers[ threadId ];
		}

		private ConsumingWorker RegisterConsumer() {
			int threadId = Thread.CurrentThread.ManagedThreadId;
			if ( !_consumers.ContainsKey( threadId ) ) {
				var consumer = new Consumer();
				lock ( Door ) {
					_consumers.Add( threadId, consumer );
				}
				return consumer;
			}
			return _consumers[ threadId ];
		}

		private ConsumingWorker RegisterCleaner() {
			int threadId = Thread.CurrentThread.ManagedThreadId;
			if ( _cleaner == null ) {
				_cleaner = new Cleaner( _bufferRange );
				_cleanerManagedId = threadId;
			}
			else if ( _cleanerManagedId != threadId ) {
				throw new InvalidOperationException( "A Cleaner is already registered. Only one allowed." );
			}

			return _cleaner;
		}

		//[ContractInvariantMethod]
		//private void ClassInvariants() {
		//      Contract.Invariant( _cleaner == null ||
		//                         _producers.Values.All( p => _cleaner.CommittedPosition - p.CommittedPosition < _buffer.Size ),
		//                         "A producer is exceeding the ringbuffer capacity." );

		//      //Contract.Invariant(_producers.ToArray().Count(p => p.WorkingPosition > p.CommittedPosition) <= 1, "Only one producer can be producing at any given time.");
		//}
	}

	public interface Worker {
		long CommittedPosition { get; }
		long WorkingPosition { get; }
		void Commit();
	}

	public interface ConsumingWorker : Worker {
		void CatchUpWith( Worker other );
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

	public class BufferRange {
		public BufferRange( int bufferSize ) {
			BufferSize = bufferSize;
			Head.CommittedPosition = Head.WorkingPosition = -1;
			Tail.CommittedPosition = Tail.WorkingPosition = -1;
		}
		public CachePaddedPosition Head;
		public CachePaddedPosition Tail;
		public int BufferSize;
	}

	public class Consumer : ConsumingWorker {
		private long _committedPosition;
		private long _workingPosition;

		public Consumer() {
			_committedPosition = _workingPosition = -1;
		}

		#region ConsumingWorker Members

		public long CommittedPosition {
			get { return _committedPosition; }
		}

		public long WorkingPosition {
			get { return _workingPosition; }
		}

		public void Commit() {
			_committedPosition = _workingPosition;
		}

		public void CatchUpWith( Worker other ) {
			_workingPosition = other.CommittedPosition;
		}

		#endregion
	}

	public class Cleaner : ConsumingWorker {
		private long _committedPosition;
		private long _workingPosition;
		private BufferRange _bufferRange;

		public Cleaner( BufferRange bufferRange ) {
			_bufferRange = bufferRange;
			_committedPosition = _workingPosition = -1;
		}

		#region ConsumingWorker Members

		public long CommittedPosition {
			get { return _committedPosition; }
		}

		public long WorkingPosition {
			get { return _workingPosition; }
		}

		public void Commit() {
			_committedPosition = _workingPosition;
			_bufferRange.Tail.CommittedPosition = _workingPosition;
		}

		public void CatchUpWith( Worker other ) {
			_workingPosition = other.CommittedPosition;
			_bufferRange.Tail.WorkingPosition = _workingPosition;
		}

		#endregion
	}

	public class ThreadSafeProducer : ProducingWorker {
		private readonly BufferRange _head;
		private readonly WaitStrategy _waitStrategy;
		private long _committedPosition;
		private long _workingPosition;

		public ThreadSafeProducer( WaitStrategy waitStrategy, BufferRange head ) {
			_head = head;
			_waitStrategy = waitStrategy;
		}

		#region ProducingWorker Members

		public long CommittedPosition {
			get { return _committedPosition; }
		}

		public long WorkingPosition {
			get { return _workingPosition; }
		}

		public void AcquireNext() {
			long original, changed;
			do {
				_waitStrategy.WaitFor( ReadyToAcquire );
				original = _head.Head.WorkingPosition;
				changed = original + 1;
			} while ( original != Interlocked.CompareExchange( ref _head.Head.WorkingPosition, changed, original ) );
			_workingPosition = changed;
		}

		public void Commit() {
			_committedPosition = _workingPosition;
			_head.Head.CommittedPosition = _committedPosition;
		}

		#endregion

		private bool ReadyToAcquire() {
			bool headIsFree = _head.Head.WorkingPosition == _head.Head.CommittedPosition;
			bool iOwnTheHead = _head.Head.WorkingPosition == _workingPosition;
			bool noWrappingWillOccur = _head.Head.WorkingPosition - _head.Tail.CommittedPosition < _head.BufferSize;
			return noWrappingWillOccur && ( headIsFree || iOwnTheHead );
		}
	}

	public static class IntExtensions {
		public static IEnumerable<int> To( this int start, int end ) {
			for ( int i = start; i <= end; i++ ) {
				yield return i;
			}
		}
	}

	internal class ConsumerHotObservable<T> : IObservable<T> {
		private readonly Func<IEnumerable<T>> _batchFetcher;
		private readonly Action _committer;
		private bool _aborting;

		public ConsumerHotObservable( Func<IEnumerable<T>> batchFetcher, Action committer ) {
			_committer = committer;
			_batchFetcher = batchFetcher;
		}

		public IDisposable Subscribe( IObserver<T> observer ) {
			while ( !_aborting ) {
				try {
					IEnumerable<T> batch = _batchFetcher();

					if ( _aborting ) break;

					foreach ( T item in batch ) {
						observer.OnNext( item );
					}
					_committer();
				}
				catch ( Exception e ) {
					observer.OnError( e );
				}
			}
			observer.OnCompleted();
			return Disposable.Create( () => _aborting = true );
		}
	}
}