// -*- mode: c++ -*-
#include "vrnetchannel.hh"
#include "mpfd.hh"

class Vrnetchannel : public Vrchannel {
  public:
    Vrnetchannel(String local_uid, String remote_uid, tamer::fd cfd);
    ~Vrnetchannel();

    void send(Json msg, tamer::event<> done);
    void receive(tamer::event<Json> done);

    void close();

  private:
    msgpack_fd cfd_;
};


Vrnetlistener::Vrnetlistener(String local_uid, Json peer_name,
                             std::mt19937& rg)
    : Vrchannel(std::move(local_uid), String()), rg_(rg) {
    if (peer_name && peer_name["port"].is_u())
        fd_ = tamer::tcp_listen(peer_name["port"].to_u());
    else if (peer_name && peer_name["path"].is_s())
        complete_unix_listen(peer_name["path"].to_s());
}

Vrnetlistener::~Vrnetlistener() {
}

tamed void Vrnetlistener::complete_unix_listen(String path) {
    tvars { struct stat st; tamer::fd checkfd; }
    // Remove an old socket that's no longer connected.
    if (stat(path.c_str(), &st) == 0
        && S_ISSOCK(st.st_mode)) {
        twait { tamer::unix_stream_connect(path, tamer::make_event(checkfd)); }
        if (checkfd.error() == -ECONNREFUSED)
            unlink(path.c_str());
    }
    fd_ = tamer::unix_stream_listen(path);
}

tamed void Vrnetlistener::connect(String peer_uid, Json peer_name,
                                  tamer::event<Vrchannel*> done) {
    tamed { struct in_addr a; tamer::fd cfd; }

    if ((peer_name["ip"].is_null() || peer_name["ip"].is_s())
        && peer_name["port"].is_u()) {
        if (peer_name["ip"].is_null())
            a.s_addr = htonl(INADDR_LOOPBACK);
        else {
            int r = inet_aton(peer_name["ip"].to_s().c_str(), &a);
            assert(r == 0);
        }
        twait { tamer::tcp_connect(a, peer_name["port"].to_u(),
                                   tamer::make_event(cfd)); }
    } else if (peer_name["path"].is_s())
        twait { tamer::unix_stream_connect(peer_name["path"].to_s(),
                                           tamer::make_event(cfd)); }

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
    } else {
        std::cerr << strerror(-cfd.error()) << "\n";
        done(nullptr);
    }
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

void Vrnetchannel::send(Json msg, tamer::event<> done) {
    cfd_.write(std::move(msg), std::move(done));
}

void Vrnetchannel::receive(tamer::event<Json> done) {
    cfd_.read(std::move(done));
}

void Vrnetchannel::close() {
    cfd_.clear();
}
