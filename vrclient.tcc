// -*- mode: c++ -*-
#include "vrclient.hh"
#include "vrchannel.hh"

Vrclient::Vrclient(std::shared_ptr<Vrchannel> me, const Vrview& config,
                   std::mt19937& rg)
    : client_seqno_(1), channel_(nullptr), me_(me), view_(config),
      stopped_(false), rg_(rg) {
    merge_view_peer_names();
}

Vrclient::~Vrclient() {
    if (channel_)
        channel_->close();      // the coroutine will delete it
    me_->close();
}

void Vrclient::merge_view_peer_names() {
    for (auto it = view_.begin(); it != view_.end(); ++it)
        if (it->peer_name)
            peer_names_[it->uid] = it->peer_name;
}

tamed void Vrclient::request(Json req, tamer::event<Json> done) {
    tamed {
        unsigned my_seqno = ++client_seqno_;
        bool retransmit = false;
    }
    at_response_.push_back(std::make_pair(my_seqno, done));
    while (done) {
        if (channel_) {
            channel_->send(Json::array(Vrchannel::m_request,
                                       Json::null,
                                       retransmit,
                                       my_seqno,
                                       req));
            retransmit = true;
        }
        twait {
            tamer::event<> e = tamer::make_event();
            at_view_change_ += e;
            tamer::at_delay(vrconstants.client_message_timeout, e);
        }
    }
}

tamed void Vrclient::connection_loop(std::shared_ptr<Vrchannel> peer) {
    tamed { Json msg; }

    while (peer.get() == channel_) {
        msg.clear();
        twait { peer->receive(tamer::make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2)
            break;
        if (stopped_) // ignore message
            continue;
        log_receive(peer) << msg << "\n";
        if (msg[0] == Vrchannel::m_handshake)
            peer->process_handshake(msg);
        else if (msg[0] == Vrchannel::m_response)
            process_response(msg);
        else if (msg[0] == Vrchannel::m_view)
            process_view(msg);
    }

    if (peer.get() == channel_)
        channel_ = nullptr;
    log_connection(peer) << "connection closed\n";
}

void Vrclient::process_response(Json msg) {
    for (int i = 2; i != msg.size(); i += 2) {
        unsigned seqno = msg[i].to_u();
        auto it = at_response_.begin();
        while (it != at_response_.end() && circular_int<unsigned>::less(it->first, seqno))
            ++it;
        if (it != at_response_.end() && it->first == seqno)
            it->second(std::move(msg[i + 1]));
        while (!at_response_.empty() && !at_response_.front().second)
            at_response_.pop_front();
    }
}

void Vrclient::process_view(Json msg) {
    Vrview view;
    if (view.assign_parse(msg[3], true, String())) {
        std::swap(view_, view);
        merge_view_peer_names();
        if (!channel_ || view_.primary().uid != channel_->remote_uid()) {
            if (channel_)
                channel_->close();
            channel_ = nullptr;
            connect(at_view_change_);
        }
    }
}

inline String Vrclient::random_replica_uid() const {
    if (view_.size() != 0) {
        unsigned i = std::uniform_int_distribution<unsigned>(0, view_.size() - 1)(rg_);
        return view_.members[i].uid;
    } else
        return String();
}

tamed void Vrclient::connect(tamer::event<> done) {
    tamed {
        String peer_uid;
        std::shared_ptr<Vrchannel> peer;
        bool ok;
    }

    while (true) {
        peer = nullptr;
        ok = false;

        if (view_.primary_index >= 0)
            peer_uid = view_.primary().uid;
        else
            peer_uid = random_replica_uid();

        twait {
            me_->connect(peer_uid, peer_names_[peer_uid],
                         tamer::make_event(peer));
        }

        if (peer) {
            peer->set_channel_uid(Vrchannel::random_uid(rg_));
            twait { peer->handshake(true, vrconstants.message_timeout,
                                    2, tamer::make_event(ok)); }
        }

        if (peer && ok) {
            channel_ = peer.get();
            connection_loop(peer);
            done();
            return;
        }

        // look for someone else
        view_.primary_index = -1;
    }
}
