using System;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Timer = System.Timers.Timer;

namespace ReliabilityPatterns
{
    public class CircuitBreaker
    {
        private readonly Timer _resetTimer;
        private int _failureCount;
        private CircuitBreakerState _state;
        private uint _threshold;

        public CircuitBreaker()
            : this(5, TimeSpan.FromSeconds(60))
        {
        }

        public CircuitBreaker(uint threshold, TimeSpan resetTimeout)
        {
            this._threshold = threshold;
            _failureCount = 0;
            _state = CircuitBreakerState.Closed;

            _resetTimer = new Timer(resetTimeout.TotalMilliseconds);
            _resetTimer.Elapsed += ResetTimerElapsed;
        }

        /// <summary>
        /// Number of failures allowed before the circuit trips.
        /// </summary>
        public uint Threshold
        {
            get { return _threshold; }
            set
            {
                if (value <= 0)
                    throw new ArgumentException("Threshold must be greater than zero");

                _threshold = value;
            }
        }

        /// <summary>
        /// The time before the circuit attempts to close after being tripped.
        /// </summary>
        public TimeSpan ResetTimeout
        {
            get { return TimeSpan.FromMilliseconds(_resetTimer.Interval); }
            set { _resetTimer.Interval = value.TotalMilliseconds; }
        }

        /// <summary>
        /// The current service level of the circuit represented as as percentage.
        /// 0 means it's offline. 100 means everything is ok.
        /// </summary>
        public double ServiceLevel
        {
            get { return ((_threshold - (double)_failureCount) / _threshold) * 100; }
        }

        public CircuitBreakerState State
        {
            get { return _state; }
        }

        public bool AllowedToAttemptExecute
        {
            get { return State == CircuitBreakerState.Closed || State == CircuitBreakerState.HalfOpen; }
        }

        public event EventHandler StateChanged;
        public event EventHandler ServiceLevelChanged;

        /// <summary>
        /// Execute the operation "protected" by the circuit breaker.
        /// </summary>
        /// <returns>The operation result.</returns>
        /// <param name="operation">Operation to execute.</param>
        /// <typeparam name="TResult">The underlying operation return type.</typeparam>
        public TResult Execute<TResult>(Func<TResult> operation)
        {
            EnsureNotOpen();

            try
            {
                // Execute operation
                var result = operation();

                OperationSucceeded();

                return result;
            }
            catch (Exception ex)
            {
                OperationFailed();
                throw new OperationFailedException("Operation failed", ex);
            }
        }

        /// <summary>
        /// Execute the operation "protected" by the circuit breaker.
        /// </summary>
        /// <param name="operation">Operation to execute.</param>
        public void Execute(Action operation)
        {
            Execute<object>(() =>
            {
                operation();
                return null;
            });
        }


        /// <summary>
        /// Execute an async operation "protected" by the circuit breaker and return an awaitable task.
        /// </summary>
        /// <returns>An awaitable task with the operation result.</returns>
        /// <param name="operation">Operation to execute.</param>
        /// <typeparam name="TResult">The underlying operation return type.</typeparam>
        public async Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> operation)
        {
            EnsureNotOpen();

            try
            {
                // Execute operation
                var result = await operation();

                OperationSucceeded();

                return result;
            }
            catch (Exception ex)
            {
                OperationFailed();
                throw new OperationFailedException("Operation failed", ex);
            }
        }

        /// <summary>
        /// Execute an async operation "protected" by the circuit breaker and return an awaitable task.
        /// </summary>
        /// <returns>Awaitable task.</returns>
        /// <param name="operation">Operation to execute.</param>
        public async Task ExecuteAsync(Func<Task> operation)
        {
            await ExecuteAsync<Task<object>>(async () =>
            {
                await operation();
                return null;
            });
        }

        /// <summary>
        /// Trips the circuit breaker. If the circuit breaker is already open,
        /// this method has no effect.
        /// </summary>
        public void Trip()
        {
            if (_state == CircuitBreakerState.Open) return;
            ChangeState(CircuitBreakerState.Open);
            _resetTimer.Start();
        }

        /// <summary>
        /// Resets the circuit breaker. If the circuit breaker is already closed,
        /// this method has no effect.
        /// </summary>
        public void Reset()
        {
            if (_state == CircuitBreakerState.Closed) return;
            ChangeState(CircuitBreakerState.Closed);
            _resetTimer.Stop();
        }

        private void ChangeState(CircuitBreakerState newState)
        {
            _state = newState;
            OnCircuitBreakerStateChanged(new EventArgs());
        }

        private void ResetTimerElapsed(object sender, ElapsedEventArgs e)
        {
            if (State != CircuitBreakerState.Open) return;
            ChangeState(CircuitBreakerState.HalfOpen);
            _resetTimer.Stop();
        }

        private void OnCircuitBreakerStateChanged(EventArgs e)
        {
            StateChanged?.Invoke(this, e);
        }

        private void OnServiceLevelChanged(EventArgs e)
        {
            ServiceLevelChanged?.Invoke(this, e);
        }

        private void EnsureNotOpen()
        {
            if (_state == CircuitBreakerState.Open)
                throw new OpenCircuitException("Circuit breaker is currently open");
        }

        private void OperationFailed()
        {
            if (_state == CircuitBreakerState.HalfOpen)
            {
                // Operation failed in a half-open state, so reopen circuit
                Trip();
            }
            else if (_failureCount < _threshold)
            {
                // Operation failed in an open state, so increment failure count and throw exception
                Interlocked.Increment(ref _failureCount);

                OnServiceLevelChanged(new EventArgs());
            }
            else if (_failureCount >= _threshold)
            {
                // Failure count has reached threshold, so trip circuit breaker
                Trip();
            }
        }

        private void OperationSucceeded()
        {
            if (_state == CircuitBreakerState.HalfOpen)
            {
                // If operation succeeded without error and circuit breaker 
                // is in a half-open state, then reset
                Reset();
            }

            if (_failureCount <= 0) return;
            // Decrement failure count to improve service level
            Interlocked.Decrement(ref _failureCount);

            OnServiceLevelChanged(new EventArgs());
        }
    }
}
