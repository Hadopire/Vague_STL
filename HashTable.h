#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>

#include "Hash.h"

namespace Vague {

// TODO(hadopire): check https://youtu.be/4cTKiTQtS_k?t=1171
//                 this video shows a few trick to speedup hashtable resizing and rehashing

template <typename T1, typename T2>
struct Pair {
    Pair() {}
    Pair(const T1 &x, const T2 &y) : first(x), second(y) {}
    Pair(T1 &&x, T2 &&y) : first(std::move(x)), second(std::move(y)) {}
    template <typename TX, typename TY>
    Pair(TX &&x, TY &&y) : first(std::move(x)), second(std::move(y)) {}

    operator Pair<const T1, T2> &() {
        // We do a reinterpret_cast to cast from a pair<key, value> to a pair<const key, value>
        // This is undefined behaviour :(
        // This works in pratice, although we should probably find another solution
        assert((reinterpret_cast<Pair<const T1, T2> &>(*this).first == first) && (reinterpret_cast<Pair<const T1, T2> &>(*this).second == second));
        return reinterpret_cast<Pair<const T1, T2> &>(*this);
    }

    T1 first;
    T2 second;
};

namespace TableDetail {
constexpr size_t minimum_capacity = 32;
constexpr float  load_factor = 0.8f;
constexpr int    last_iterator = -2;

static size_t next_power_of_2(size_t n) {
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

static size_t msb(size_t n) {
    size_t ret = 0;
    while (n >>= 1) {
        ++ret;
    }

    return ret;
}

template <typename Key, typename Value, typename Hash, typename KeyEqual, bool isFlat>
class HashTable {
private:
    template <bool, typename Dummy>
    struct TBucket;

    template <typename Dummy>
    struct TBucket<true, Dummy> {
        int              delta = -1;
        Pair<Key, Value> pair;

        TBucket() {}

        bool is_empty() const {
            return delta == -1;
        }

        void insert(int d, Pair<Key, Value> &&p) {
            assert(delta == -1);
            delta = d;
            pair = std::forward<Pair<Key, Value>>(p);
        }

        void erase() {
            assert(delta != -1);
            delta = -1;
        }

        Pair<Key, Value> &get_pair() {
            assert(delta != -1);
            return pair;
        }

        const Pair<Key, Value> &get_pair() const {
            assert(delta != -1);
            return pair;
        }

        friend void swap(TBucket &a, TBucket &b) {
            using std::swap;
            swap(a.delta, b.delta);
            swap(a.pair, b.pair);
        }
    };

    template <typename Dummy>
    struct TBucket<false, Dummy> {
        int               delta = -1;
        Pair<Key, Value> *pair;

        bool is_empty() const {
            return delta == -1;
        }

        void insert(int d, Pair<Key, Value> &&p) {
            assert(delta == -1);
            delta = d;
            pair = new Pair<Key, Value>(std::forward<Pair<Key, Value>>(p));
        }

        void erase() {
            assert(delta != -1);
            delta = -1;
            delete pair;
        }

        Pair<Key, Value> &get_pair() {
            assert(delta != -1);
            return *pair;
        }

        const Pair<Key, Value> &get_pair() const {
            assert(delta != -1);
            return *pair;
        }

        friend void swap(TBucket &a, TBucket &b) {
            using std::swap;
            swap(a.delta, b.delta);
            swap(a.pair, b.pair);
        }
    };

    using Bucket = TBucket<isFlat, int>;

    size_t m_capacity = 0;
    size_t m_maxLoad = 0;
    size_t m_load = 0;
    size_t m_mask = 0;
    size_t m_probe_count = 0;

    Bucket        *m_buckets = nullptr;
    const Hash     m_hasher;
    const KeyEqual m_key_equal;

public:
    template <typename T>
    class TableIterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = T *;
        using reference = T &;
        using const_reference = T const &;

    public:
        TableIterator() = default;
        TableIterator(Bucket *ptr) {
            m_ptr = ptr;
        }
        TableIterator(const TableIterator &) = default;
        ~TableIterator() {}

        bool operator==(const TableIterator &rhs) const {
            return m_ptr == rhs.m_ptr;
        }

        bool operator!=(const TableIterator &rhs) const {
            return m_ptr != rhs.m_ptr;
        }

        TableIterator &operator++() {
            do {
                ++m_ptr;
            } while (m_ptr->is_empty());

            return *this;
        }

        TableIterator operator++(int) {
            auto tmp(*this);
            ++(*this);
            return tmp;
        }

        T &operator*() {
            return m_ptr->get_pair();
        }

        const T &operator*() const {
            return m_ptr->get_pair();
        }

        T *operator->() {
            return (T *)&m_ptr->get_pair();
        }

        const T *operator->() const {
            return (const T *)&m_ptr->get_pair();
        }

    private:
        Bucket *m_ptr;
    };

    using iterator = TableIterator<Pair<const Key, Value>>;
    using const_iterator = TableIterator<const Pair<const Key, Value>>;

    struct SleepingIterator {
        friend class HashTable<Key, Value, Hash, KeyEqual, isFlat>;

        operator iterator() {
            while (m_ptr->is_empty()) {
                ++m_ptr;
            }
            return iterator(m_ptr);
        }

        operator const_iterator() {
            while (m_ptr->is_empty()) {
                ++m_ptr;
            }
            return iterator(m_ptr);
        }

    private:
        SleepingIterator(Bucket *ptr)
            : m_ptr(ptr) {
        }
        Bucket *m_ptr;
    };

    HashTable() {
    }

    HashTable(size_t capacity) {
        resize(TableDetail::next_power_of_2(capacity < TableDetail::minimum_capacity ? TableDetail::minimum_capacity : capacity));
    }

    HashTable(const HashTable &other) {
        resize(other.m_capacity);

        Bucket *end = last();
        for (Bucket *it = m_buckets, *src = other.m_buckets; it != end; ++src, ++it) {
            if (!src->is_empty()) {
                Pair<Key, Value> newPair(src->get_pair().first, src->get_pair().second);
                it->insert(src->delta, std::move(newPair));
            }
        }
        m_load = other.m_load;
    }

    HashTable(HashTable &&other) {
        swap(*this, other);
    }

    ~HashTable() {
        free();
    }

    void free() {
        if (m_buckets) {
            Bucket *bucket = m_buckets;
            Bucket *end = last();
            for (; bucket != end; ++bucket) {
                if (!bucket->is_empty()) {
                    bucket->erase();
                }
            }
            delete[] m_buckets;
            m_buckets = nullptr;
        }
    }

    void reserve(size_t n) {
        if (n > m_capacity) {
            resize(TableDetail::next_power_of_2(n));
        }
    }

    Value &operator[](const Key &key) {
        return emplace(key);
    }

    Value &operator[](Key &&key) {
        return emplace(std::move(key));
    }

    HashTable &operator=(HashTable &rhs) {
        *this = HashTable(rhs);
        return *this;
    }

    HashTable &operator=(HashTable &&rhs) {
        swap(*this, rhs);
        return *this;
    }

    friend void swap(HashTable &first, HashTable &second) {
        using std::swap;
        swap(first.m_buckets, second.m_buckets);
        swap(first.m_capacity, second.m_capacity);
        swap(first.m_load, second.m_load);
        swap(first.m_mask, second.m_mask);
        swap(first.m_maxLoad, second.m_maxLoad);
        swap(first.m_probe_count, second.m_probe_count);
    }

    template <typename K, typename... Args>
    Value &emplace(K &&key, Args &&...args) {
        size_t  location = m_hasher(key) & m_mask;
        Bucket *current = m_buckets + location;
        Bucket *end = current + m_probe_count;
        int     delta = 0;
        for (; current != end && delta <= current->delta; ++current, ++delta) {
            if (delta == current->delta && m_key_equal(key, current->get_pair().first)) {
                return current->get_pair().second;
            }
        }

        return insert(location + delta, delta, std::forward<K>(key), std::forward<Args>(args)...);
    }

    SleepingIterator erase(const Key &key) {
        using std::swap;

        size_t  location = m_hasher(key) & m_mask;
        Bucket *bucket = find_bucket(key, location);
        Bucket *end = last();
        if (bucket == nullptr)
            return end;

        bucket->erase();
        --m_load;

        Bucket *current = bucket;
        Bucket *next = current + 1;
        for (; next != end && !next->is_empty() && next->delta; ++current, ++next) {
            --next->delta;
            swap(*current, *next);
        }
        return bucket;
    }

    size_t count(const Key &key) const {
        return find_bucket(key, m_hasher(key) & m_mask) != nullptr;
    }

    size_t size() const {
        return m_load;
    }

    iterator begin() {
        iterator it = m_buckets;
        if (m_buckets->is_empty())
            ++it;
        return it;
    }

    const_iterator begin() const {
        const_iterator it = m_buckets;
        if (m_buckets->is_empty())
            ++it;
        return it;
    }

    const_iterator cbegin() const {
        return begin();
    }

    iterator end() {
        return m_buckets + m_capacity + m_probe_count;
    }

    const_iterator end() const {
        return m_buckets + m_capacity + m_probe_count;
    }

    const_iterator cend() const {
        return end();
    }

    iterator find(const Key &key) {
        size_t  location = m_hasher(key) & m_mask;
        Bucket *bucket = find_bucket(key, location);
        if (bucket != nullptr)
            return bucket;

        return end();
    }

    const_iterator find(const Key &key) const {
        size_t  location = m_hasher(key) & m_mask;
        Bucket *bucket = find_bucket(key, location);
        if (bucket != nullptr)
            return bucket;

        return end();
    }

private:
    Bucket *find_bucket(const Key &key, size_t location) const {
        Bucket *current = m_buckets + location;
        Bucket *end = current + m_probe_count;
        if (current == nullptr)
            return nullptr;

        for (int delta = 0; delta <= current->delta && current <= end; ++current, ++delta) {
            if (delta == current->delta && m_key_equal(key, current->get_pair().first)) {
                return current;
            }
        }

        return nullptr;
    }

    Bucket *last() {
        return m_buckets + m_capacity + m_probe_count;
    }

    template <typename K, typename... Args>
    Value &insert(size_t location, int delta, K &&key, Args &&...args) {
        using std::swap;

        if (m_load == m_maxLoad) {
            grow();
            return emplace(std::forward<K>(key), std::forward<Args>(args)...);
        }

        Bucket *current = m_buckets + location;
        Bucket  new_bucket;
        Bucket *ret;
        if (current->is_empty()) {
            current->insert(delta, std::move(Pair<Key, Value>(Key(std::forward<K>(key)), Value(std::forward<Args>(args)...))));
            ++m_load;
            return current->get_pair().second;
        } else {
            new_bucket.insert(delta, std::move(Pair<Key, Value>(Key(std::forward<K>(key)), Value(std::forward<Args>(args)...))));
            swap(*current, new_bucket);
            ret = current;
        }

        while (true) {
            if (new_bucket.delta > m_probe_count) {
                swap(*ret, new_bucket);
                grow();
                location = m_hasher(new_bucket.get_pair().first) & m_mask;
                current = m_buckets + location;
                new_bucket.delta = 0;

                while (new_bucket.delta <= current->delta) {
                    ++new_bucket.delta;
                    ++current;
                }

                swap(*current, new_bucket);
                if (new_bucket.is_empty()) {
                    ++m_load;
                    return current->get_pair().second;
                } else {
                    ret = current;
                }
            } else if (current->is_empty()) {
                swap(*current, new_bucket);
                ++m_load;
                return ret->get_pair().second;
            } else if (current->delta < new_bucket.delta) {
                swap(*current, new_bucket);
            }

            ++current;
            ++new_bucket.delta;
        }
    }

    void grow() {
        if (m_capacity < TableDetail::minimum_capacity) {
            resize();
        } else {
            assert((m_capacity << 1) > m_capacity);
            resize(m_capacity << 1);
        }
    }

    void resize(size_t capacity = TableDetail::minimum_capacity) {
        size_t oldCapacity = m_capacity;
        size_t oldProbeCount = m_probe_count;

        m_capacity = capacity;
        m_maxLoad = size_t(m_capacity * TableDetail::load_factor);
        m_mask = m_capacity - 1;
        m_probe_count = TableDetail::msb(m_capacity);

        Bucket *oldBuckets = m_buckets;
        if (m_capacity != 0) {
            // +probecount so we don't have to wrap around when probing
            // +1 for the end() iterator
            m_buckets = new Bucket[m_capacity + m_probe_count + 1];
            m_buckets[m_capacity + m_probe_count].delta = TableDetail::last_iterator;
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

    void rehash(Bucket *src, size_t count) {
        using std::swap;

        Bucket *end = src + count;
        for (Bucket *it = src; it != end; ++it) {
            if (!it->is_empty()) {
                it->delta = 0;
                size_t  location = m_hasher(it->get_pair().first) & m_mask;
                Bucket *current = m_buckets + location;
                while (true) {
                    if (current->is_empty()) {
                        current->delta = it->delta;
                        current->pair = std::move(it->pair);
                        break;
                    } else if (current->delta < it->delta) {
                        swap(*it, *current);
                    }
                    ++current;
                    ++it->delta;
                }
            }
        }
    }
};
} // namespace TableDetail

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using HashTable = TableDetail::HashTable<Key, Value, Hash, KeyEqual, sizeof(Pair<Key, Value>) <= 64 && std::is_move_constructible<Pair<Key, Value>>::value>;

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using FlatHashTable = TableDetail::HashTable<Key, Value, Hash, KeyEqual, true>;

template <typename Key, typename Value, typename Hash = Vague::Hash<Key>, typename KeyEqual = std::equal_to<Key>>
using NodeHashTable = TableDetail::HashTable<Key, Value, Hash, KeyEqual, false>;

} // namespace Vague