#ifndef LOGGER_HH
#define LOGGER_HH 1
#include <iostream>
#include "straccum.hh"

class Logger {
  public:
    inline Logger(std::ostream& str);
    inline StringAccum& buffer();
    inline std::ostream& stream();
    inline void stream(std::ostream& stream);
    inline void flush();
    inline Logger& operator()();
    inline Logger& operator()(bool active);
    inline bool active() const;
    inline bool quiet() const;
    inline void set_quiet(bool quiet);
    inline unsigned frequency() const;
    inline void set_frequency(unsigned frequency);
  private:
    std::ostream* stream_;
    StringAccum buffer_;
    bool active_;
    bool quiet_;
    unsigned frequency_;
    unsigned count_;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
};

extern Logger logger;


inline Logger::Logger(std::ostream& str)
    : stream_(&str), active_(true), frequency_(0), count_(0) {
}

inline StringAccum& Logger::buffer() {
    return buffer_;
}

inline std::ostream& Logger::stream() {
    return *stream_;
}

inline void Logger::stream(std::ostream& stream) {
    stream_ = &stream;
}

inline void Logger::flush() {
    if (buffer_.length()) {
        stream_->write(buffer_.begin(), buffer_.length());
        buffer_.clear();
    }
    stream_->flush();
}

inline Logger& Logger::operator()() {
    if (frequency_ != 0) {
        active_ = (count_ == 0);
        if (++count_ == frequency_)
            count_ = 0;
    }
    return *this;
}

inline Logger& Logger::operator()(bool active) {
    active_ = active;
    return *this;
}

inline bool Logger::active() const {
    return active_;
}

inline bool Logger::quiet() const {
    return quiet_;
}

inline void Logger::set_quiet(bool q) {
    quiet_ = q;
}

inline unsigned Logger::frequency() const {
    return frequency_;
}

inline void Logger::set_frequency(unsigned frequency) {
    frequency_ = frequency;
    if (count_ > frequency_)
        count_ = 0;
    if (count_ == 0)
        active_ = true;
}


std::ostream& operator<<(std::ostream& str, const timeval& tv);
StringAccum& operator<<(StringAccum& sa, const timeval& tv);

template <typename T>
inline Logger& operator<<(Logger& logger, T&& x) {
    if (logger.active()) {
        logger.buffer() << std::forward<T>(x);
        if (logger.buffer().length() && logger.buffer().back() == '\n')
            logger.flush();
    }
    return logger;
}

#endif
