// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.CompilerServices.Unsafe;

namespace _1brc
{
    [DebuggerDisplay("Count = {" + nameof(Count) + "}")]
    public unsafe class FixedDictionary<TKey, TValue> : IReadOnlyCollection<KeyValuePair<TKey, TValue>>
        where TKey : struct, IEquatable<TKey>
    {
        private const int CAPACITY = 10_000;
        private static readonly int Size = HashHelpers.GetPrime(CAPACITY);
        private static readonly ulong FastModMultiplier = HashHelpers.GetFastModMultiplier((uint)Size);

        private int _count;

        // 1-based index into _entries; 0 means empty
        private readonly int[] _buckets = new int[Size];
        private readonly Entry[] _entries = new Entry[Size];

        [StructLayout(LayoutKind.Auto)]
        private struct Entry
        {
            public uint hashCode;
            public int next;
            public TKey key;
            public TValue value;
        }

        /// <summary>
        /// Count of entries in the dictionary.
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Clears the dictionary. Note that this invalidates any active enumerators.
        /// </summary>
        public void Clear()
        {
            int count = _count;
            if (count > 0)
            {
                Debug.Assert(_buckets != null, "_buckets should be non-null");
                Debug.Assert(_entries != null, "_entries should be non-null");

                Array.Clear(_buckets);
                Array.Clear(_entries, 0, count);
                _count = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(TKey key)
        {
            Entry[] entries = _entries;

            uint hashCode = (uint)key.GetHashCode(); // Constrained call

            // 1BRC context: using GetAtUnsafe to avoid bound checks here is actually visible ~25-30 msecs.

            uint fastMod = HashHelpers.FastMod(hashCode, (uint)Size, FastModMultiplier);
            int bucket = _buckets.GetAtUnsafe(fastMod);

            int i = bucket - 1; // Value in _buckets is 1-based

            while (true)
            {
                if ((uint)i >= (uint)entries.Length) // Eliminates bound checks
                {
                    break;
                }

                if (entries[i].hashCode == hashCode && entries[i].key.Equals(key))
                {
                    return ref entries[i].value!;
                }

                i = entries[i].next;
            }

            int index = _count++;

            // No need for 1BRC
            // int index;
            // if ((index = _count++) == entries.Length)
            // {
            //     Throw();
            //
            //     [MethodImpl(MethodImplOptions.NoInlining)]
            //     static void Throw() => throw new InvalidOperationException("Exceeded FixedDictionary capacity.");
            // }

            ref Entry entry = ref entries.GetAtUnsafe((uint)index);

            entry.hashCode = hashCode;
            entry.next = (bucket - 1); // Value in _buckets is 1-based
            entry.key = key;
            entry.value = default!;

            _buckets.GetAtUnsafe(fastMod) = index + 1; // Value in _buckets is 1-based

            return ref entry.value!;
        }

        public Enumerator GetEnumerator() => new(this);
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() => new Enumerator(this);
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Enumerator
        /// </summary>
        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private readonly FixedDictionary<TKey, TValue> _dictionary;
            private int _index;
            private int _count;
            private KeyValuePair<TKey, TValue> _current;

            internal Enumerator(FixedDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _index = 0;
                _count = _dictionary._count;
                _current = default;
            }

            /// <summary>
            /// Move to next
            /// </summary>
            public bool MoveNext()
            {
                if (_count == 0)
                {
                    _current = default;
                    return false;
                }

                _count--;

                while ((int)_dictionary._entries[_index].next < -1)
                    _index++;

                _current = new KeyValuePair<TKey, TValue>(
                    _dictionary._entries[_index].key,
                    _dictionary._entries[_index++].value);
                return true;
            }

            /// <summary>
            /// Get current value
            /// </summary>
            public KeyValuePair<TKey, TValue> Current => _current;

            object IEnumerator.Current => _current;

            void IEnumerator.Reset()
            {
                _index = 0;
                _count = _dictionary._count;
                _current = default;
            }

            /// <summary>
            /// Dispose the enumerator
            /// </summary>
            public void Dispose()
            {
            }
        }

        // Even though different customizations of a standard dictionary (such as above)
        // are quite common, the following HashHelpers class rarely changes at all and 
        // never requires any changes. .NET 8 introduces a new UnsafeAccessorAttribute (https://learn.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.unsafeaccessorattribute?view=net-8.0)
        // allowing access to non-public members of a class.
        // 
        // However it does not work with non-public or static classes, and HashHelpers is both.
        // There is an API proposal for `UnsafeAccessorTypeAttribute` (https://github.com/dotnet/runtime/issues/90081)
        // that will allow to just use the .NET internal class directly.

        public static class HashHelpers
        {
            public const int HashPrime = 101;

            private static readonly int[] Primes =
            {
                3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
                1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
                17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
                187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
                1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
            };

            public static bool IsPrime(int candidate)
            {
                if ((candidate & 1) != 0)
                {
                    int limit = (int)Math.Sqrt(candidate);
                    for (int divisor = 3; divisor <= limit; divisor += 2)
                    {
                        if ((candidate % divisor) == 0)
                            return false;
                    }

                    return true;
                }

                return candidate == 2;
            }

            public static int GetPrime(int min)
            {
                if (min < 0)
                    throw new ArgumentException("Capacity Overflow");

                foreach (int prime in Primes)
                {
                    if (prime >= min)
                        return prime;
                }

                for (int i = (min | 1); i < int.MaxValue; i += 2)
                {
                    if (IsPrime(i) && ((i - 1) % HashPrime != 0))
                        return i;
                }

                return min;
            }

            public static ulong GetFastModMultiplier(uint divisor) => ulong.MaxValue / divisor + 1;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint FastMod(uint value, uint divisor, ulong multiplier)
            {
                Debug.Assert(divisor <= int.MaxValue);
                uint highbits = (uint)(((((multiplier * value) >> 32) + 1) * divisor) >> 32);
                Debug.Assert(highbits == value % divisor);
                return highbits;
            }
        }
    }

    public static class VectorExtensions
    {
        /// <summary>
        /// Get an <paramref name="array"/> element at <paramref name="index"/> in a very unsafe way.
        /// There are no checks for null or bounds, the validity of the call must be ensured before using this method.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref T GetAtUnsafe<T>(this T[] array, nuint index)
        {
            // Depends on implementation. Should be stable since .NET Core and going forward.
            const nuint arrayOffset = 8;

            Debug.Assert((uint)index < array.Length, "GetAtUnsafe: (uint)index < array.Length");
            return ref Add(ref AddByteOffset(ref As<Box<T>>(array)!.Value, arrayOffset), index);
        }
        
        public class Box<T>
        {
            public T Value = default!;

            public Box()
            {
            }

            public Box(T value) => Value = value;

            public static explicit operator T(Box<T> boxed) => boxed.Value;
        }
    }
}