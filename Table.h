#pragma once

#include <cassert>
#include <cstdint>
#include <string>
#include <cstring>

#include "Hash.h"

namespace Vague {

template <typename T1, typename T2>
struct Pair {
    template <typename TX = T1, typename TY = T2,
        typename std::enable_if<std::is_default_constructible<TX>::value && std::is_default_constructible<TY>::value, int>::type = 0>
    Pair()
        : first()
        , second()
    {
    }
    Pair(const T1& x, const T2& y)
        : first(x)
        , second(y)
    {
    }
    Pair(T1&& x, T2&& y)
        : first(std::move(x))
        , second(std::move(y))
    {
    }
    template <typename TX, typename TY>
    Pair(TX&& x, TY&& y)
        : first(std::move(x))
        , second(std::move(y))
    {
    }

    operator Pair<const T1, T2> &()
    {
        // We do a reinterpret_cast to cast from a pair<key, value> to a pair<const key, value>
        // This is undefined behaviour :(
        // This works in pratice, although we should probably find another solution
        assert((reinterpret_cast<Pair<const T1, T2>&>(*this).first == first) && (reinterpret_cast<Pair<const T1, T2>&>(*this).second == second));
        return reinterpret_cast<Pair<const T1, T2>&>(*this);
    }

    T1 first;
    T2 second;
};

namespace TableDetail {
    constexpr size_t minimumCapacity = 32;
    constexpr float loadFactor = 0.8f;
    constexpr int lastIterator = -2;

    static size_t nextHighestPowerOf2(size_t n)
    {
        --n;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    static size_t msb(size_t n)
    {
        size_t ret = 0;
        while (n >>= 1) {
            ++ret;
        }

        return ret;
    }

    template <typename Key, typename Value, typename Hash, typename KeyEqual, bool isFlat>
    class Table {
    private:
        template <bool, typename Dummy = int>
        struct TBucket;

        template <typename Dummy>
        struct TBucket<true, Dummy> {
            int m_delta;
            Pair<Key, Value> m_pair;

            TBucket()
                : m_delta(-1)
            {
            }
            ~TBucket() { }

            bool isEmpty() const
            {
                return m_delta == -1;
            }

            void insert(int d, Pair<Key, Value>&& p)
            {
                assert(m_delta == -1);
                m_delta = d;
                m_pair = std::forward<Pair<Key, Value>>(p);
            }

            void erase()
            {
                assert(m_delta != -1);
                m_delta = -1;
            }

            Pair<Key, Value>& pair()
            {
                assert(m_delta != -1);
                return m_pair;
            }

            const Pair<Key, Value>& pair() const
            {
                assert(m_delta != -1);
                return m_pair;
            }

            friend void swap(TBucket& a, TBucket& b)
            {
                using std::swap;
                swap(a.m_delta, b.m_delta);
                swap(a.m_pair, b.m_pair);
            }
        };

        template <typename Dummy>
        struct TBucket<false, Dummy> {
            int m_delta;
            Pair<Key, Value>* m_pair;

            TBucket()
                : m_delta(-1)
            {
            }
            ~TBucket() { }

            bool isEmpty() const
            {
                return m_delta == -1;
            }

            void insert(int d, Pair<Key, Value>&& p)
            {
                assert(m_delta == -1);
                m_delta = d;
                m_pair = new Pair<Key, Value>(std::forward<Pair<Key, Value>>(p));
            }

            void erase()
            {
                assert(m_delta != -1);
                m_delta = -1;
                delete m_pair;
            }

            Pair<Key, Value>& pair()
            {
                assert(m_delta != -1);
                return *m_pair;
            }

            const Pair<Key, Value>& pair() const
            {
                assert(m_delta != -1);
                return *m_pair;
            }

            friend void swap(TBucket& a, TBucket& b)
            {
                using std::swap;
                swap(a.m_delta, b.m_delta);
                swap(a.m_pair, b.m_pair);
            }
        };

        using Bucket = TBucket<isFlat, int>;

        size_t m_capacity;
        size_t m_maxLoad;
        size_t m_load;
        size_t m_mask;
        size_t m_probeCount;

        Bucket* m_buckets;
        const Hash m_hasher;
        const KeyEqual m_keyEqual;

    public:
        template <typename T>
        class TableIterator {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = T;
            using difference_type = std::ptrdiff_t;
            using pointer = T*;
            using reference = T&;
            using const_reference = T const&;

        public:
            TableIterator() = default;
            TableIterator(Bucket* ptr)
            {
                m_ptr = ptr;
            }
            TableIterator(const TableIterator&) = default;
            ~TableIterator() { }

            bool operator==(const TableIterator& rhs) const
            {
                return m_ptr == rhs.m_ptr;
            }

            bool operator!=(const TableIterator& rhs) const
            {
                return m_ptr != rhs.m_ptr;
            }

            TableIterator& operator++()
            {
                do {
                    ++m_ptr;
                } while (m_ptr->isEmpty());

                return *this;
            }

            TableIterator operator++(int)
            {
                auto tmp(*this);
                ++(*this);
                return tmp;
            }

            T& operator*()
            {
                return m_ptr->pair();
            }

            const T& operator*() const
            {
                return m_ptr->pair();
            }

            T* operator->()
            {
                return m_ptr->pair();
            }

            const T* operator->() const
            {
                return m_ptr->pair();
            }

        private:
            Bucket* m_ptr;
        };

        using iterator = TableIterator<Pair<const Key, Value>>;
        using const_iterator = TableIterator<const Pair<const Key, Value>>;

        struct SleepingIterator {
            friend class Table<Key, Value, Hash, KeyEqual, isFlat>;

            operator iterator()
            {
                while (m_ptr->isEmpty()) {
                    ++m_ptr;
                }
                return iterator(m_ptr);
            }

            operator const_iterator()
            {
                while (m_ptr->isEmpty()) {
                    ++m_ptr;
                }
                return iterator(m_ptr);
            }

        private:
            SleepingIterator(Bucket* ptr)
                : m_ptr(ptr)
            {
            }
            Bucket* m_ptr;
        };

        Table()
            : m_hasher()
            , m_keyEqual()
            , m_load(0)
            , m_buckets(nullptr)
        {
            resize(0);
        }

        Table(size_t capacity)
            : m_hasher()
            , m_keyEqual()
            , m_load(0)
            , m_buckets(nullptr)
        {
            resize(TableDetail::nextHighestPowerOf2(capacity));
        }

        Table(const Table& other)
            : m_hasher()
            , m_keyEqual()
            , m_load(0)
            , m_buckets(nullptr)
        {
            resize(other.m_capacity);

            Bucket* end = last();
            for (Bucket *it = m_buckets, *src = other.m_buckets; it != end; ++src, ++it) {
                if (!src->isEmpty()) {
                    Pair<Key, Value> newPair(src->pair().first, src->pair().second);
                    it->insert(src->m_delta, std::move(newPair));
                }
            }
            m_load = other.m_load;
        }

        Table(Table&& other)
            : m_hasher()
            , m_keyEqual()
            , m_load(0)
            , m_buckets(nullptr)
        {
            swap(*this, other);
        }

        ~Table()
        {
            if (m_buckets) {
                Bucket* bucket = m_buckets;
                Bucket* end = last();
                for (; bucket != end; ++bucket) {
                    if (!bucket->isEmpty()) {
                        bucket->erase();
                    }
                }
                delete[] m_buckets;
            }
        }

        void reserve(size_t n)
        {
            if (n > m_capacity) {
                resize(TableDetail::nextHighestPowerOf2(n));
            }
        }

        Value& operator[](const Key& key)
        {
            return emplace(key);
        }

        Value& operator[](Key&& key)
        {
            return emplace(std::move(key));
        }

        Table& operator=(Table rhs)
        {
            swap(*this, rhs);
            return *this;
        }

        Table& operator=(Table&& rhs)
        {
            swap(*this, rhs);
            return *this;
        }

        friend void swap(Table& first, Table& second)
        {
            using std::swap;
            swap(first.m_buckets, second.m_buckets);
            swap(first.m_capacity, second.m_capacity);
            swap(first.m_load, second.m_load);
            swap(first.m_mask, second.m_mask);
            swap(first.m_maxLoad, second.m_maxLoad);
            swap(first.m_probeCount, second.m_probeCount);
        }

        template <typename K, typename... Args>
        Value& emplace(K&& key, Args&&... args)
        {
            size_t location = m_hasher(key) & m_mask;
            Bucket* current = m_buckets + location;
            Bucket* end = current + m_probeCount;
            int m_delta = 0;
            for (; current != end && m_delta <= current->m_delta; ++current, ++m_delta) {
                if (m_delta == current->m_delta && m_keyEqual(key, current->pair().first)) {
                    return current->pair().second;
                }
            }

            return insert(location + m_delta, m_delta, std::forward<K>(key), std::forward<Args>(args)...);
        }

        SleepingIterator erase(const Key& key)
        {
            using std::swap;

            size_t location = m_hasher(key) & m_mask;
            Bucket* bucket = findBucket(key, location);
            Bucket* end = last();
            if (bucket == nullptr)
                return end;

            bucket->erase();
            --m_load;

            Bucket* current = bucket;
            Bucket* next = current + 1;
            for (; next != end && !next->isEmpty() && next->m_delta; ++current, ++next) {
                --next->m_delta;
                swap(*current, *next);
            }
            return bucket;
        }

        size_t count(const Key& key) const
        {
            return findBucket(key, m_hasher(key) & m_mask) != nullptr;
        }

        size_t size() const
        {
            return m_load;
        }

        iterator begin()
        {
            iterator it = m_buckets;
            if (m_buckets->isEmpty())
                ++it;
            return it;
        }

        const_iterator begin() const
        {
            const_iterator it = m_buckets;
            if (m_buckets->isEmpty())
                ++it;
            return it;
        }

        const_iterator cbegin() const
        {
            return begin();
        }

        iterator end()
        {
            return m_buckets + m_capacity + m_probeCount;
        }

        const_iterator end() const
        {
            return m_buckets + m_capacity + m_probeCount;
        }

        const_iterator cend() const
        {
            return end();
        }

        iterator find(const Key& key)
        {
            size_t location = m_hasher(key) & m_mask;
            Bucket* bucket = findBucket(key, location);
            if (bucket != nullptr)
                return bucket;

            return last();
        }

        const_iterator find(const Key& key) const
        {
            size_t location = m_hasher(key) & m_mask;
            Bucket* bucket = findBucket(key, location);
            if (bucket != nullptr)
                return bucket;

            return last();
        }

    private:
        Bucket* findBucket(const Key& key, size_t location) const
        {
            Bucket* current = m_buckets + location;
            Bucket* end = current + m_probeCount;
            for (int m_delta = 0; m_delta <= current->m_delta && current <= end; ++current, ++m_delta) {
                if (m_delta == current->m_delta && m_keyEqual(key, current->pair().first)) {
                    return current;
                }
            }

            return nullptr;
        }

        Bucket* last()
        {
            return m_buckets + m_capacity + m_probeCount;
        }

        template <typename K, typename... Args>
        Value& insert(size_t location, int m_delta, K&& key, Args&&... args)
        {
            using std::swap;

            if (m_load == m_maxLoad) {
                grow();
                return emplace(std::forward<K>(key), std::forward<Args>(args)...);
            }

            Bucket* current = m_buckets + location;
            Bucket newBucket;
            Bucket* ret;
            if (current->isEmpty()) {
                current->insert(m_delta, std::move(Pair<Key, Value>(Key(std::forward<K>(key)), Value(std::forward<Args>(args)...))));
                ++m_load;
                return current->pair().second;
            } else {
                newBucket.insert(m_delta, std::move(Pair<Key, Value>(Key(std::forward<K>(key)), Value(std::forward<Args>(args)...))));
                swap(*current, newBucket);
                ret = current;
            }

            while (true) {
                if (newBucket.m_delta > m_probeCount) {
                    swap(*ret, newBucket);
                    grow();
                    location = m_hasher(newBucket.pair().first) & m_mask;
                    current = m_buckets + location;
                    newBucket.m_delta = 0;

                    while (newBucket.m_delta <= current->m_delta) {
                        ++newBucket.m_delta;
                        ++current;
                    }

                    swap(*current, newBucket);
                    if (newBucket.isEmpty()) {
                        ++m_load;
                        return current->pair().second;
                    } else {
                        ret = current;
                    }
                } else if (current->isEmpty()) {
                    swap(*current, newBucket);
                    ++m_load;
                    return ret->pair().second;
                } else if (current->m_delta < newBucket.m_delta) {
                    swap(*current, newBucket);
                }

                ++current;
                ++newBucket.m_delta;
            }
        }

        void grow()
        {
            if (m_capacity < TableDetail::minimumCapacity) {
                resize();
            } else {
                assert((m_capacity << 1) > m_capacity);
                resize(m_capacity << 1);
            }
        }

        void resize(size_t capacity = TableDetail::minimumCapacity)
        {
            size_t oldCapacity = m_capacity;
            size_t oldProbeCount = m_probeCount;

            m_capacity = capacity;
            m_maxLoad = m_capacity * TableDetail::loadFactor;
            m_mask = m_capacity - 1;
            m_probeCount = TableDetail::msb(m_capacity);

            Bucket* oldBuckets = m_buckets;
            if (m_capacity != 0) {
                // +probecount so we don't have to wrap around when probing
                // +1 for the end() iterator
                m_buckets = new Bucket[m_capacity + m_probeCount + 1];
                m_buckets[m_capacity + m_probeCount].m_delta = TableDetail::lastIterator;
            } else {
                m_buckets = nullptr;
                m_mask = 0;
            }

            if (oldBuckets) {
                if (m_load) {
                    rehash(oldBuckets, oldCapacity + oldProbeCount);
                }
                delete[] oldBuckets;
            }
        }

        void rehash(Bucket* src, size_t count)
        {
            using std::swap;

            Bucket* end = src + count;
            for (Bucket* it = src; it != end; ++it) {
                if (!it->isEmpty()) {
                    it->m_delta = 0;
                    size_t location = m_hasher(it->pair().first) & m_mask;
                    Bucket* current = m_buckets + location;
                    while (true) {
                        if (current->isEmpty()) {
                            current->m_delta = it->m_delta;
                            current->m_pair = std::move(it->m_pair);
                            break;
                        } else if (current->m_delta < it->m_delta) {
                            swap(*it, *current);
                        }
                        ++current;
                        ++it->m_delta;
                    }
                }
            }
        }
    };
}

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using Table = TableDetail::Table<Key, Value, Hash, KeyEqual, sizeof(Pair<Key, Value>) <= 32 && std::is_move_constructible<Pair<Key, Value>>::value>;

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using FlatTable = TableDetail::Table<Key, Value, Hash, KeyEqual, true>;

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using NodeTable = TableDetail::Table<Key, Value, Hash, KeyEqual, false>;

}