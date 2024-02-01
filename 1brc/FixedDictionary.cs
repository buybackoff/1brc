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
    public unsafe class FixedDictionary<TKey, TValue> : IReadOnlyCollection<KeyValuePair<TKey, TValue>>, IDisposable
        where TKey : struct, IEquatable<TKey>
    {
        // Pick a Prime for Size
        // private static readonly int[] Primes =
        // {
        //     3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        //     1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        //     17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        //     187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        //     1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        // };

        private const int Size = 43627;

        private int _count;

        // 1-based index into _entries; 0 means empty
        private readonly int[] _buckets = new int[Size];
        private readonly Entry[] _entries = new Entry[Size];
        private readonly byte* _keys = (byte*)Marshal.AllocHGlobal(Size * 101);
        private int _keysLength;

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
        public void Update(TKey key, nint value)
        {
            if (typeof(TValue) == typeof(Summary))
            {
                if (!TryUpdate(key, value))
                    As<TValue, Summary>(ref GetValueRefOrAddDefault(key)).Apply(value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdate(TKey key, nint value)
        {
            Entry[] entries = _entries;

            uint hashCode = (uint)key.GetHashCode();
            uint fastMod = hashCode % Size;
            int bucket = _buckets.GetAtUnsafe(fastMod);

            nuint i = (uint)bucket - 1;
            if (i < (uint)entries.Length)
            {
                if (typeof(TValue) == typeof(Summary))
                {
                    if (entries.GetAtUnsafe(i).key.Equals(key))
                    {
                        As<TValue, Summary>(ref entries.GetAtUnsafe(i).value).Apply(value);
                        return true;
                    }
                }
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public ref TValue GetValueRefOrAddDefault(TKey key)
        {
            Entry[] entries = _entries;
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            uint bucketIndex = hashCode % Size;
            int bucket = _buckets.GetAtUnsafe(bucketIndex);
            nuint i = (uint)bucket - 1; // Value in _buckets is 1-based

            while (true)
            {
                if (i >= (uint)entries.Length) // Eliminates bound checks
                    break;

                if (entries.GetAtUnsafe(i).hashCode == hashCode && entries.GetAtUnsafe(i).key.Equals(key))
                    return ref entries.GetAtUnsafe(i).value!;

                i = (uint)entries.GetAtUnsafe(i).next;
            }

            return ref Add(key, hashCode, bucketIndex);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private ref TValue Add(TKey key, uint hashCode, uint fastMod)
        {
            int index = _count++;

            if (typeof(TKey) == typeof(Utf8Span))
            {
                // Within the type check, this is no-op for JIT, no boxing happens. 
                key = (TKey)(object)Copykey((Utf8Span)(object)key);

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                Utf8Span Copykey(Utf8Span utf8Span)
                {
                    var targetPtr = _keys + _keysLength;
                    // For short keys (of length 1 and 3) we touch ; as a part of hash, e.g. read 1 byte beyond Utf8Span. Need to copy ; as well here.
                    int utf8SpanLength = utf8Span.Length <= 3 ? (int)utf8Span.Length + 1 : (int)utf8Span.Length;
                    // CopyBlockUnaligned(targetPtr, utf8Span.Pointer, (uint)utf8SpanLength);
                    var target = new Span<byte>(targetPtr, utf8SpanLength);
                    var source = new Span<byte>(utf8Span.Pointer, utf8SpanLength);
                    source.CopyTo(target);
                    _keysLength += utf8SpanLength;
                    return new Utf8Span(targetPtr, utf8Span.Length);
                }
            }

            // No need for 1BRC
            // int index;
            // if ((index = _count++) == entries.Length)
            // {
            //     Throw();
            //
            //     [MethodImpl(MethodImplOptions.NoInlining)]
            //     static void Throw() => throw new InvalidOperationException("Exceeded FixedDictionary capacity.");
            // }

            ref Entry entry = ref _entries.GetAtUnsafe((uint)index);

            entry.hashCode = hashCode;
            entry.next = (_buckets.GetAtUnsafe(fastMod) - 1); // Value in _buckets is 1-based
            entry.key = key;

            if (typeof(TValue) == typeof(Summary))
            {
                entry.value = (TValue)(object)new Summary() { Count = 0, Sum = 0, Max = -1000, Min = 1000 };
            }
            else
            {
                entry.value = default!;
            }

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

        private void ReleaseUnmanagedResources()
        {
            Marshal.FreeHGlobal((nint)_keys);
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        ~FixedDictionary()
        {
            ReleaseUnmanagedResources();
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
            const nuint arrayOffset = 8; // Depends on implementation. Should be stable since .NET Core and going forward.
            Debug.Assert((uint)index < array.Length, "GetAtUnsafe: (uint)index < array.Length");
            return ref Add(ref AddByteOffset(ref As<Box<T>>(array).Value, arrayOffset), index);
        }

        public class Box<T>
        {
            public T Value = default!;
        }
    }
}