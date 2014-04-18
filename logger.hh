#ifndef LOGGER_HH
#define LOGGER_HH 1
#include <iostream>
class StringAccum;

class Logger {
  public:
    inline Logger(std::ostream& str);
    inline std::ostream& stream() const;
    inline Logger& operator()();
    inline Logger& operator()(bool active);
    inline bool active() const;
    inline unsigned frequency() const;
    inline void set_frequency(unsigned frequency);
  private:
    std::ostream& stream_;
    bool active_;
    unsigned frequency_;
    unsigned count_;
};

extern Logger logger;


inline Logger::Logger(std::ostream& str)
    : stream_(str), active_(true), frequency_(0), count_(0) {
}

inline std::ostream& Logger::stream() const {
    return stream_;
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
    if (logger.active())
        logger.stream() << std::forward<T>(x);
    return logger;
}

#endif
