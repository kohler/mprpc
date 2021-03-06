#ifndef VRCHANNEL_HH
#define VRCHANNEL_HH 1
#include <tamer/tamer.hh>
#include <random>
#include <memory>
#include "json.hh"
#include "logger.hh"
#include "vrlog.hh"

class Vrchannel {
  public:
    inline Vrchannel(String local_uid, String remote_uid)
        : local_uid_(std::move(local_uid)), remote_uid_(std::move(remote_uid)),
          connection_version_(0), error_viewno_(0), error_viewno_at_(0) {
    }
    virtual ~Vrchannel() {
    }

    inline const String& local_uid() const {
        return local_uid_;
    }
    inline const String& remote_uid() const {
        return remote_uid_;
    }
    inline const String& channel_uid() const {
        return channel_uid_;
    }
    inline void set_channel_uid(String x) {
        assert(channel_uid_.empty() || channel_uid_ == x);
        channel_uid_ = std::move(x);
    }
    inline unsigned connection_version() const {
        return connection_version_;
    }
    virtual Json status() const;

    virtual void connect(String peer_uid, Json peer_name,
                         tamer::event<std::shared_ptr<Vrchannel> > done);
    virtual void receive_connection(tamer::event<std::shared_ptr<Vrchannel> > done);

    bool check_view_response(viewnumber_t viewno);

    void send_handshake(bool want_reply);
    bool check_handshake(const Json& msg) const;
    void process_handshake(const Json& msg);

    void handshake(bool active_end, double message_timeout, double timeout,
                   tamer::event<bool> done);

    inline void send(Json msg);
    virtual void send(Json msg, tamer::event<> done);
    virtual void receive(tamer::event<Json> done);

    virtual void close();

    template <typename RNG> static String random_uid(RNG& rng);

    static const String m_request;
    static const String m_response;
    static const String m_commit;
    static const String m_ack;
    static const String m_handshake;
    static const String m_join;
    static const String m_view;
    static const String m_kill;
    static const String m_error;

  protected:
    String local_uid_;
    String remote_uid_;
    String channel_uid_;
    unsigned connection_version_;
    viewnumber_t error_viewno_;
    double error_viewno_at_;

    class closure__handshake__bddQb_;
    void handshake(closure__handshake__bddQb_&);
};

template <typename RNG>
String Vrchannel::random_uid(RNG& rg) {
    uint64_t x = std::uniform_int_distribution<uint64_t>()(rg);
    return String((char*) &x, 6).encode_base64();
}

inline void Vrchannel::send(Json msg) {
    send(std::move(msg), tamer::event<>());
}

inline bool Vrchannel::check_view_response(viewnumber_t viewno) {
    if (viewno != error_viewno_
        || error_viewno_at_ + 5 < tamer::drecent()) {
        error_viewno_ = viewno;
        error_viewno_at_ = tamer::drecent();
        return true;
    } else
        return false;
}

inline Logger& log_connection(const String& local_uid,
                              const String& remote_uid,
                              const char* ctype = " -- ") {
    return logger() << tamer::recent() << ":" << local_uid << ctype
                    << (remote_uid ? remote_uid : "[]") << ": ";
}

inline Logger& log_connection(const Vrchannel* ep,
                              const char* ctype = " -- ") {
    logger() << tamer::recent() << ":" << ep->local_uid() << ctype;
    if (const String& x = ep->remote_uid())
        logger << x;
    else
        logger << "[]";
    if (const String& x = ep->channel_uid())
        logger << " (" << x << ")";
    return logger << ": ";
}

inline Logger& log_send(const Vrchannel* ep) {
    return log_connection(ep, " -> ");
}

inline Logger& log_receive(const Vrchannel* ep) {
    return log_connection(ep, " <- ");
}

inline Logger& log_connection(std::shared_ptr<const Vrchannel> ep,
                              const char* ctype = " -- ") {
    return log_connection(ep.get(), ctype);
}

inline Logger& log_send(std::shared_ptr<const Vrchannel> ep) {
    return log_send(ep.get());
}

inline Logger& log_receive(std::shared_ptr<const Vrchannel> ep) {
    return log_receive(ep.get());
}

#endif
