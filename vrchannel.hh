#ifndef VRCHANNEL_HH
#define VRCHANNEL_HH 1
#include <tamer/tamer.hh>
#include <random>
#include "json.hh"
#include "logger.hh"

class Vrchannel {
  public:
    inline Vrchannel(String local_uid, String remote_uid)
        : local_uid_(std::move(local_uid)), remote_uid_(std::move(remote_uid)),
          connection_version_(0) {
    }
    virtual ~Vrchannel() {
    }

    inline const String& local_uid() const {
        return local_uid_;
    }
    inline const String& remote_uid() const {
        return remote_uid_;
    }
    inline const String& connection_uid() const {
        return connection_uid_;
    }
    inline void set_connection_uid(String x) {
        assert(connection_uid_.empty() || connection_uid_ == x);
        connection_uid_ = std::move(x);
    }
    inline unsigned connection_version() const {
        return connection_version_;
    }

    virtual void connect(String peer_uid, Json peer_name,
                         tamer::event<Vrchannel*> done);
    virtual void receive_connection(tamer::event<Vrchannel*> done);

    void handshake(bool active_end, double message_timeout, double timeout,
                   tamer::event<bool> done);
    bool check_handshake(const Json& msg) const;
    void process_handshake(const Json& msg, bool reply);

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
    String connection_uid_;
    unsigned connection_version_;

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


inline Logger& log_connection(const String& local_uid,
                              const String& remote_uid,
                              const char* ctype = " <-> ") {
    return logger() << tamer::recent() << ":"
                    << local_uid << ctype << remote_uid << ": ";
}

inline Logger& log_connection(const Vrchannel* ep,
                              const char* ctype = " <-> ") {
    logger() << tamer::recent() << ":"
             << ep->local_uid() << ctype << ep->remote_uid();
    if (ep->connection_uid())
        logger << " (" << ep->connection_uid() << ")";
    return logger << ": ";
}

inline Logger& log_send(const Vrchannel* ep) {
    return log_connection(ep, " -> ");
}

inline Logger& log_receive(const Vrchannel* ep) {
    return log_connection(ep, " <- ");
}

#endif
