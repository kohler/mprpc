// -*- mode: c++ -*-
#include "vrclient.hh"
#include "vrchannel.hh"

Vrclient::Vrclient(Vrchannel* me, std::mt19937& rg)
    : uid_(Vrchannel::random_uid(rg)), client_seqno_(1), me_(me),
      channel_(nullptr), stopped_(false), rg_(rg) {
}

Vrclient::~Vrclient() {
    for (auto it = at_response_.begin(); it != at_response_.end(); ++it)
        it->second.unblock();
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
        twait { tamer::at_delay(vrconstants.client_message_timeout,
                                tamer::make_event()); }
    }
}

tamed void Vrclient::connection_loop(Vrchannel* peer) {
    tamed { Json msg; }

    while (peer == channel_) {
        msg.clear();
        twait { peer->receive(tamer::make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2)
            break;
        if (stopped_) // ignore message
            continue;
        log_receive(peer) << msg << "\n";
        if (msg[0] == Vrchannel::m_handshake)
            peer->process_handshake(msg, true);
        else if (msg[0] == Vrchannel::m_response)
            process_response(msg);
        else if (msg[0] == Vrchannel::m_view)
            process_view(msg);
    }

    log_connection(peer) << "connection closed\n";
    delete peer;
    if (peer == channel_)
        channel_ = nullptr;
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
    if (view_.assign(msg[2], String())
        && (!channel_ || view_.primary().uid != channel_->remote_uid())) {
        if (channel_)
            channel_->close();
        channel_ = nullptr;
        connect(view_.primary().uid, view_.primary().peer_name,
                tamer::event<>());
    }
}

tamed void Vrclient::connect(String peer_uid, Json peer_name,
                             tamer::event<> done) {
    tamed { Vrchannel* peer; bool ok; int tries = 0; }
    while (1) {
        peer = nullptr;
        ok = false;

        twait { me_->connect(peer_uid, peer_name, tamer::make_event(peer)); }

        if (peer) {
            peer->set_connection_uid(Vrchannel::random_uid(rg_));
            twait { peer->handshake(true, vrconstants.message_timeout,
                                    10000, tamer::make_event(ok)); }
        }

        if (peer && ok) {
            channel_ = peer;
            connection_loop(peer);
            done();
            return;
        }

        delete peer;
        // every 8th try, look for someone else
        ++tries;
        if (tries % 8 == 7 && view_.size()) {
            unsigned i = std::uniform_int_distribution<unsigned>(0, view_.size() - 1)(rg_);
            peer_uid = view_.members[i].uid;
            peer_name = view_.members[i].peer_name;
        }
    }
}
