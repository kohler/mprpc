#ifndef VRCHANNEL_HH
#define VRCHANNEL_HH 1
#include <tamer/tamer.hh>
#include <random>
#include "json.hh"

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
    virtual Json local_name() const;
    virtual Json remote_name() const;

    virtual void connect(String peer_uid, Json peer_name,
                         tamer::event<Vrchannel*> done);
    virtual void receive_connection(tamer::event<Vrchannel*> done);

    virtual void send(Json msg);
    virtual void receive(tamer::event<Json> done);
    virtual void close();

    template <typename RNG> static String random_uid(RNG& rng);

  protected:
    String local_uid_;
    String remote_uid_;
    String connection_uid_;
    unsigned connection_version_;
};

template <typename RNG>
String Vrchannel::random_uid(RNG& rg) {
    uint64_t x = std::uniform_int_distribution<uint64_t>()(rg);
    return String((char*) &x, 6).encode_base64();
}

#endif
