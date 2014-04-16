// -*- mode: c++ -*-
#include "vrnetchannel.hh"
#include "mpfd.hh"

class Vrnetchannel : public Vrchannel {
  public:
    Vrnetchannel(String local_uid, String remote_uid, tamer::fd cfd);
    ~Vrnetchannel();

    void send(Json msg);
    void receive(tamer::event<Json> done);

    void close();

  private:
    msgpack_fd cfd_;
};


Vrnetlistener::Vrnetlistener(String local_uid, unsigned port,
                             std::mt19937& rg)
    : Vrchannel(std::move(local_uid), String()),
      fd_(tamer::tcp_listen(port)), port_(port), rg_(rg) {
}

Vrnetlistener::~Vrnetlistener() {
}

tamed void Vrnetlistener::connect(String peer_uid, Json peer_name,
                                  tamer::event<Vrchannel*> done) {
    tamed { struct in_addr a; tamer::fd cfd; }

    if (peer_name["ip"].is_null())
        a.s_addr = htonl(INADDR_LOOPBACK);
    else {
        assert(peer_name["ip"].is_s());
        int r = inet_aton(peer_name["ip"].to_s().c_str(), &a);
        assert(r == 0);
    }
    assert(peer_name["port"].is_u());

    twait { tcp_connect(a, peer_name["port"].to_u(), tamer::make_event(cfd)); }

    if (cfd) {
        Vrnetchannel* c = new Vrnetchannel(local_uid(), peer_uid, std::move(cfd));
        done(c);
    } else
        done(nullptr);
}

typedef union {
    struct sockaddr s;
    struct sockaddr_in sin;
} my_sockaddr_union;

tamed void Vrnetlistener::receive_connection(tamer::event<Vrchannel*> done) {
    tamed {
        my_sockaddr_union sa;
        socklen_t salen;
        tamer::fd cfd;
    }

    twait { fd_.accept(&sa.s, &salen, tamer::make_event(cfd)); }

    if (cfd) {
        Vrnetchannel* c = new Vrnetchannel(local_uid(), String(), std::move(cfd));
        done(c);
    } else
        done(nullptr);
}

void Vrnetlistener::close() {
    fd_.close();
}


Vrnetchannel::Vrnetchannel(String local_uid, String remote_uid, tamer::fd cfd)
    : Vrchannel(std::move(local_uid), std::move(remote_uid)),
      cfd_(std::move(cfd)) {
}

Vrnetchannel::~Vrnetchannel() {
}

void Vrnetchannel::send(Json msg) {
    cfd_.write(msg);
}

void Vrnetchannel::receive(tamer::event<Json> done) {
    cfd_.read(done);
}

void Vrnetchannel::close() {
    cfd_.clear();
}
