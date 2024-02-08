/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.Data;
using System.Collections;

namespace QuantConnect.IEX
{
    /// <summary>
    /// The new enumerator wrapper for this subscription request
    /// 
    /// </summary>
    public class EnumeratorWrapper : IEnumerator<BaseData>
    {
        private readonly IEnumerator<BaseData> _underlying;

        private Func<bool> _subscriptionInterruptionCheck;

        /// <inheritdoc/>
        public BaseData Current => _underlying.Current;

        /// <inheritdoc/>
        object IEnumerator.Current => _underlying.Current;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumeratorWrapper"/> class.
        /// </summary>
        /// <param name="underlying">The underlying enumerator to wrap.</param>
        /// <param name="subscriptionInterruptionCheck">A function delegate that checks for subscription interruption.</param>
        public EnumeratorWrapper(IEnumerator<BaseData> underlying, Func<bool> subscriptionInterruptionCheck)
        {
            _underlying = underlying;
            _subscriptionInterruptionCheck = subscriptionInterruptionCheck;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _underlying.Dispose();
        }

        /// <inheritdoc/>
        public bool MoveNext()
        {
            if (_subscriptionInterruptionCheck())
            {
                throw new Exception($"The Subscription process was interrupted or encountered an error. Unable to proceed");
            }

            return _underlying.MoveNext();
        }

        /// <inheritdoc/>
        public void Reset()
        {
            _underlying.Reset();
        }
    }
}
