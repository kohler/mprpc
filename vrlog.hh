#ifndef VRLOG_HH
#define VRLOG_HH 1
#include "json.hh"
#include "msgpack.hh"
#include "circular_int.hh"
#include <iostream>
#include <deque>

typedef circular_int<unsigned> viewnumber_t;
typedef viewnumber_t::difference_type viewnumberdiff_t;
typedef circular_int<unsigned> lognumber_t;
typedef lognumber_t::difference_type lognumberdiff_t;

struct Vrlogitem {
    viewnumber_t viewno;
    String client_uid;
    unsigned client_seqno;
    Json request;
    Json response;

    Vrlogitem() {
    }
    Vrlogitem(viewnumber_t v, String cuid, unsigned cseqno, Json req)
        : viewno(v), client_uid(std::move(cuid)), client_seqno(cseqno),
          request(std::move(req)) {
    }
    bool is_real() const {
        return client_uid;
    }
    inline bool request_equals(const Vrlogitem& x) const;
};


template <typename T, typename I>
class Vrlog {
  public:
    typedef typename std::deque<T>::size_type size_type;
    typedef circular_int<I> index_type;

    inline Vrlog();
    explicit inline Vrlog(index_type first);
    inline Vrlog(index_type first, index_type last, T x);

    inline bool empty() const;
    inline size_type size() const;
    inline index_type first() const;
    inline index_type last() const;

    typedef typename std::deque<T>::iterator iterator;
    typedef typename std::deque<T>::const_iterator const_iterator;
    inline const_iterator begin() const;
    inline const_iterator position(index_type i) const;
    inline const_iterator end() const;
    inline iterator begin();
    inline iterator position(index_type i);
    inline iterator end();

    inline T& operator[](index_type i);
    inline const T& operator[](index_type i) const;

    inline void push_back(const T& x);
    inline void push_back(T&& x);
    template <typename... Args>
    inline void emplace_back(Args&&... args);
    inline void pop_front();

    inline void resize(size_type n);
    inline void clear();
    inline void set_first(index_type i);

  private:
    index_type first_;
    std::deque<T> log_;
};


inline bool Vrlogitem::request_equals(const Vrlogitem& x) const {
    return is_real()
        && viewno == x.viewno
        && client_uid == x.client_uid
        && client_seqno == x.client_seqno
        && (request.is_primitive() || x.request.is_primitive()
            ? request == x.request
            : msgpack::unparse(request) == msgpack::unparse(x.request));
}

std::ostream& operator<<(std::ostream& str, const Vrlogitem& x);


template <typename T, typename I>
inline Vrlog<T, I>::Vrlog()
    : first_(0) {
}

template <typename T, typename I>
inline Vrlog<T, I>::Vrlog(index_type first)
    : first_(first) {
}

template <typename T, typename I>
inline Vrlog<T, I>::Vrlog(index_type first, index_type last, T x)
    : first_(first), log_(last - first, std::move(x)) {
}

template <typename T, typename I>
inline bool Vrlog<T, I>::empty() const {
    return log_.empty();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::size() const -> size_type {
    return log_.size();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::first() const -> index_type {
    return first_;
}

template <typename T, typename I>
inline auto Vrlog<T, I>::last() const -> index_type {
    return first_ + log_.size();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::begin() const -> const_iterator {
    return log_.begin();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::position(index_type i) const -> const_iterator {
    size_type x = i - first_;
    assert(x <= log_.size());
    return log_.begin() + x;
}

template <typename T, typename I>
inline auto Vrlog<T, I>::end() const -> const_iterator {
    return log_.end();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::begin() -> iterator {
    return log_.begin();
}

template <typename T, typename I>
inline auto Vrlog<T, I>::position(index_type i) -> iterator {
    size_type x = i - first_;
    assert(x <= log_.size());
    return log_.begin() + x;
}

template <typename T, typename I>
inline auto Vrlog<T, I>::end() -> iterator {
    return log_.end();
}

template <typename T, typename I>
inline T& Vrlog<T, I>::operator[](index_type i) {
    size_type x = i - first_;
    assert(x < log_.size());
    return log_[x];
}

template <typename T, typename I>
inline const T& Vrlog<T, I>::operator[](index_type i) const {
    size_type x = i - first_;
    assert(x < log_.size());
    return log_[x];
}

template <typename T, typename I>
inline void Vrlog<T, I>::push_back(const T& x) {
    log_.push_back(x);
}

template <typename T, typename I>
inline void Vrlog<T, I>::push_back(T&& x) {
    log_.push_back(std::move(x));
}

template <typename T, typename I> template <typename... Args>
inline void Vrlog<T, I>::emplace_back(Args&&... args) {
    log_.emplace_back(std::forward<Args>(args)...);
}

template <typename T, typename I>
inline void Vrlog<T, I>::pop_front() {
    ++first_;
    log_.pop_front();
}

template <typename T, typename I>
inline void Vrlog<T, I>::resize(size_type n) {
    log_.resize(n);
}

template <typename T, typename I>
inline void Vrlog<T, I>::clear() {
    log_.clear();
}

template <typename T, typename I>
inline void Vrlog<T, I>::set_first(index_type first) {
    assert(empty());
    first_ = first;
}

#endif
