// -*- mode: c++ -*-
#include "clp.h"
#include "vrreplica.hh"
#include "vrstate.hh"
#include <algorithm>
#include <fstream>

Vrreplica::Vrreplica(Vrstate* state, const Vrview& config,
                     Vrchannel* me, std::mt19937& rg)
    : state_(state), me_(me),
      decideno_(0), commitno_(0), ackno_(0), sackno_(0),
      stopped_(false), commit_sent_at_(0),
      rg_(rg) {
    assert(config.empty() || config.count(uid()));

    // adopt channel names from config
    for (auto it = config.members.begin(); it != config.members.end(); ++it)
        if (it->peer_name)
            channels_[it->uid].name = it->peer_name;
    channels_[uid()].c = me;

    // current view == just me
    cur_view_ = Vrview::make_singular(config.group_name(), uid());
    next_view_ = cur_view_;
    between_views_ = between_views_primary_sent_ = false;
    listen_loop();
}

void Vrreplica::dump(std::ostream& out) const {
    timeval now = tamer::now();
    out << now << ":" << uid() << ": " << unparse_view_state()
        << " " << cur_view_.members_json()
        << " p@" << cur_view_.primary_index << "\n";
}

String Vrreplica::unparse_view_state() const {
    StringAccum sa;
    sa << "v#" << cur_view_.viewno
       << (cur_view_.me_primary() ? "p" : "");
    if (next_view_.viewno != cur_view_.viewno) {
        sa << "<v#" << next_view_.viewno
           << (next_view_.me_primary() ? "p" : "")
           << ":" << next_view_.nprepared;
        if (next_view_.me_primary())
            sa << "." << next_view_.nconfirmed;
        sa << ">";
    }
    sa << " ";
    if (!log_.first() && log_.empty()) {
        assert(!commitno_);
        sa << "-";
    } else {
        sa << first_logno() << ":";
        if (decideno() != first_logno())
            sa << decideno();
        sa << ":";
        if (commitno() != decideno())
            sa << commitno();
        sa << ":";
        if (last_logno() != commitno())
            sa << last_logno();
    }
    return sa.take_string();
}

void Vrreplica::verify_state() const {
    if (decideno_ > commitno_
        || decideno_ > ackno_
        || commitno_ > ackno_
        || ackno_ > sackno_)
        logger() << tamer::recent() << ":" << uid() << ": bad lognos "
                 << unparse_view_state() << "\n";
    assert(decideno_ <= commitno_);
    assert(decideno_ <= ackno_);
    assert(commitno_ <= ackno_);
    assert(ackno_ <= sackno_);

    assert(next_view_.viewno != cur_view_.viewno || !between_views_);
    for (auto it = next_view_.members.begin(); it != next_view_.members.end(); ++it)
        assert(it->uid == uid() || it->prepared() == it->has_ackno());
}

tamed void Vrreplica::listen_loop() {
    tamed { Vrchannel* peer; }
    while (1) {
        twait { me_->receive_connection(make_event(peer)); }
        if (!peer)
            break;
        connection_handshake(peer, false, tamer::event<>());
    }
}

tamed void Vrreplica::connect(String peer_uid, event<> done) {
    tamed {
        Vrchannel* peer;
        channel_type* ch = &channels_[peer_uid];
        tamer::ref_monitor mon(ref_);
    }

    // does peer already exist?
    if (ch->c) {
        done();
        return;
    }

    // are we already connecting?
    if (ch->connecting) {
        ch->wait += std::move(done);
        return;
    }

    ch->connecting = true;
    ch->wait = std::move(done);
    ch->backoff = 0.05;

    // random delay to reduce likelihood of simultaneous connection,
    // which we currently handle poorly
    twait { tamer::at_delay(rand01() / 100, make_event()); }

    // connected during delay?
    if (!mon)
        return;
    if (ch->c) {
        assert(!ch->connecting);
        return;
    }

    while (!ch->c && ch->wait) {
        log_connection(uid(), peer_uid) << "connecting\n";
        twait { me_->connect(peer_uid, ch->name, make_event(peer)); }
        if (peer) {
            assert(peer->remote_uid() == peer_uid);
            peer->set_connection_uid(Vrchannel::random_uid(rg_));
            twait { connection_handshake(peer, true, make_event()); }
        } else if (mon) {
            twait { tamer::at_delay(ch->backoff, make_event()); }
            ch->backoff = std::min(ch->backoff * 2, 10.0);
        } else
            return;
    }

    ch->connecting = false;
    ch->wait();
}

tamed void Vrreplica::join(String peer_uid, event<> done) {
    tamed { Vrchannel* ep; tamer::ref_monitor mon(ref_); }
    while (mon && next_view_.size() == 1) {
        if ((ep = channels_[peer_uid].c)) {
            ep->send(Json::array(Vrchannel::m_join, Json::null));
            twait {
                at_view(next_view_.viewno + 1,
                        tamer::add_timeout(k_.message_timeout, make_event()));
            }
        } else
            twait { connect(peer_uid, make_event()); }
    }
    done();
}

void Vrreplica::join(String peer_uid, Json peer_name, event<> done) {
    if ((peer_name = Vrview::clean_peer_name(std::move(peer_name))))
        channels_[peer_uid].name = std::move(peer_name);
    return join(peer_uid, done);
}

void Vrreplica::join(const Vrview& config, event<> done) {
    assert(config.group_name() == cur_view_.group_name());
    for (auto it = config.members.begin(); it != config.members.end(); ++it) {
        if (it->peer_name)
            channels_[it->uid].name = it->peer_name;
        if (it->uid != uid())
            join(it->uid, done);
    }
}

tamed void Vrreplica::connection_handshake(Vrchannel* peer, bool active_end,
                                           tamer::event<> done) {
    tamed { bool ok = false; }
    twait { peer->handshake(active_end, k_.message_timeout,
                            k_.handshake_timeout, make_event(ok)); }
    if (!ok)
        delete peer;
    else {
        String peer_uid = peer->remote_uid();
        channel_type& ch = channels_[peer_uid];

        if (ch.c) {
            String old_cuid = ch.c->connection_uid();
            if (old_cuid < peer->connection_uid())
                log_connection(peer) << "preferring old connection (" << old_cuid << ")\n";
            else {
                log_connection(peer) << "dropping old connection (" << old_cuid << ")\n";
                ch.c->close();
                ch.c = peer;
            }
        } else
            ch.c = peer;

        ch.wait();
        ch.connecting = false;
        connection_loop(peer);
    }
    done();
}

tamed void Vrreplica::connection_loop(Vrchannel* peer) {
    tamed { Json msg; }

    while (1) {
        verify_state();
        msg.clear();
        twait { peer->receive(make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2)
            break;
        if (stopped_) // ignore message
            continue;
        log_receive(peer) << msg << " " << unparse_view_state() << "\n";
        if (msg[0] == Vrchannel::m_handshake)
            peer->process_handshake(msg, true);
        else if (msg[0] == Vrchannel::m_request)
            process_request(peer, msg);
        else if (msg[0] == Vrchannel::m_commit)
            process_commit(peer, msg);
        else if (msg[0] == Vrchannel::m_ack)
            process_ack(peer, msg);
        else if (msg[0] == Vrchannel::m_join)
            process_join(peer, msg);
        else if (msg[0] == Vrchannel::m_view)
            process_view(peer, msg);
        else if (msg[0] == Vrchannel::m_kill)
            exit(0);
    }

    log_connection(peer) << "connection closed\n";
    if (channels_[peer->remote_uid()].c == peer)
        channels_[peer->remote_uid()].c = nullptr;
    delete peer;
}

void Vrreplica::at_view(viewnumber_t viewno, tamer::event<> done) {
    if (viewno > cur_view_.viewno)
        at_view_.push_back(std::make_pair(viewno, std::move(done)));
    else
        done();
}

void Vrreplica::at_store(lognumber_t storeno, tamer::event<> done) {
    if (storeno > last_logno())
        at_store_.push_back(std::make_pair(storeno, std::move(done)));
    else
        done();
}

void Vrreplica::at_commit(viewnumber_t commitno, tamer::event<> done) {
    if (commitno > commitno_)
        at_commit_.push_back(std::make_pair(commitno, std::move(done)));
    else
        done();
}

inline String Vrreplica::view_why(const String& why) const {
    StringAccum sa;
    sa << why << "@" << tamer::recent();
    return sa.take_string();
}

void Vrreplica::process_view(Vrchannel* who, const Json& msg) {
    Json payload = msg[2];
    Vrview v;
    if (!v.assign_parse(payload, true, uid())
        || !v.count(who->remote_uid())
        || v.group_name() != cur_view_.group_name()) {
        who->send(Json::array(Vrchannel::m_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);
    int vcompare = vdiff == 0 ? next_view_.compare(v) : 0;
    int want_send;
    if (vdiff == 0 && vcompare > 0) {
        // advance view number
        next_view_.advance();
        start_view_change();
        return;
    } else if (vdiff < 0
               || (vdiff == 0 && vcompare < 0)
               || !next_view_.shared_quorum(v))
        // respond with current view, take no other action
        want_send = 2;
    else if (vdiff == 0) {
        cur_view_.prepare(who->remote_uid(), payload, false);
        next_view_.prepare(who->remote_uid(), payload, true);
        if (payload["log"]
            && next_view_.me_primary()) {
            if (cur_view_.viewno != next_view_.viewno)
                process_view_transfer_log(who, payload);
            else
                process_view_check_log(who, payload);
        }
        want_send = !payload["ack"] && !payload["confirm"];
    } else {
        // start new view
        next_view_ = v;
        initialize_next_view();
        cur_view_.prepare(who->remote_uid(), payload, false);
        next_view_.prepare(who->remote_uid(), payload, true);
        broadcast_view(view_why("new"));
        want_send = 0;
    }

    if (cur_view_.nprepared > cur_view_.f()
        && next_view_.nprepared > next_view_.f()
        && cur_view_.viewno != next_view_.viewno)
        between_views_ = true;
    if (between_views_
        && (next_view_.me_primary()
            || next_view_.primary().prepared())
        && !between_views_primary_sent_) {
        if (next_view_.me_primary())
            next_view_.prepare(uid(), Json::object("confirm", true), true);
        else if (!want_send
                 || who->remote_uid() != next_view_.primary().uid)
            send_view(next_view_.primary().uid, view_why("confirm"));
        between_views_primary_sent_ = true;
    }
    if (next_view_.nconfirmed > next_view_.f()
        && next_view_.me_primary()
        && want_send != 2) {
        if (cur_view_.viewno != next_view_.viewno)
            primary_adopt_view_change(who);
        else
            send_commit_log(cur_view_.find_pointer(who->remote_uid()),
                            commitno(), last_logno());
    } else if (want_send)
        send_view(who, view_why("send"));
}

void Vrreplica::process_view_transfer_log(Vrchannel* who, Json& payload) {
    assert(payload["logno"].is_u()
           && payload["log"].is_a()
           && payload["log"].size() % 4 == 0
           && next_view_.me_primary());
    lognumber_t logno = payload["logno"].to_u();
    assert(logno <= last_logno());
    const Json& log = payload["log"];
    lognumber_t matching_logno = logno + log.size();

    // Combine log from payload with current log. New entries go into
    // next_log_, which is combined with log_ when the new view is adopted.
    // Although we could put new entries directly into log_, this makes
    // checking more difficult, since it effectively can make a log entry
    // appear to be committed before its time.
    //
    // Example: 5 replicas, n0-n4. n0 & n1 have l#1<@v#0>, n2 & n3 have
    // l#1<@v#1>, n4 has neither. New master is n4. If, during the view
    // change, n0's entry arrives first, and is added to n4's true log, then
    // all of a sudden it looks like l#1<@v#0> was replicated 3 times, i.e.,
    // it committed.
    for (int i = 0; i != log.size(); i += 4, ++logno) {
        Vrlogitem li(log[i].to_u(), log[i+1].to_s(), log[i+2].to_u(), log[i+3]);
        if (logno < log_.first())
            continue;
        Vrlogitem* lix;
        if (logno < log_.last())
            lix = &log_[logno];
        else if (!next_log_.empty() && logno < next_log_.last())
            lix = &next_log_[logno];
        else {
            if (next_log_.empty())
                next_log_.set_first(log_.last());
            assert(logno == next_log_.last());
            next_log_.push_back(std::move(li));
            continue;
        }
        if (lix->empty() || lix->viewno < li.viewno) {
            *lix = std::move(li);
            next_view_.reduce_matching_logno(logno);
        } else if (lix->viewno == li.viewno)
            assert(lix->client_uid == li.client_uid
                   && lix->client_seqno == li.client_seqno);
        else /* log diverged */
            matching_logno = std::min(logno, matching_logno);
    }

    next_view_.set_matching_logno(who->remote_uid(), matching_logno);
}

void Vrreplica::process_view_check_log(Vrchannel* who, Json& payload) {
    assert(payload["logno"].is_u()
           && payload["log"].is_a()
           && payload["log"].size() % 4 == 0);
    lognumber_t logno = payload["logno"].to_u();
    assert(logno <= last_logno());
    const Json& log = payload["log"];
    for (int i = 0; i != log.size() && logno < last_logno(); i += 4, ++logno)
        if (logno >= log_.first()
            && log[i].to_u() != log_[logno].viewno)
            break;
    next_view_.set_matching_logno(who->remote_uid(), logno);
}

void Vrreplica::primary_adopt_view_change(Vrchannel* who) {
    // transfer next_log_ into log_
    for (lognumber_t i = next_log_.first(); i != next_log_.last(); ++i)
        if (i == log_.last())
            log_.push_back(std::move(next_log_[i]));
        else if (log_[i].empty() || log_[i].viewno < next_log_[i].viewno)
            log_[i] = std::move(next_log_[i]);
        else if (log_[i].viewno > next_log_[i].viewno)
            next_view_.reduce_matching_logno(i);
    next_log_.clear();

    // truncate log if there are gaps
    for (lognumber_t i = commitno_; i != last_logno(); ++i)
        if (log_[i].empty()) {
            log_.resize(i - log_.first());
            break;
        }

    // no one's logs are valid beyond the end of the current log
    next_view_.reduce_matching_logno(last_logno());
    // our log is valid to the end of the current log
    ackno_ = sackno_ = last_logno();

    // actually switch to new view
    cur_view_ = next_view_;
    between_views_ = between_views_primary_sent_ = false;
    process_at_number(cur_view_.viewno, at_view_);
    primary_keepalive_loop();

    // send log to replicas
    for (auto it = cur_view_.members.begin();
         it != cur_view_.members.end(); ++it)
        if (it->confirmed())
            send_commit_log(&*it, it->ackno(), last_logno());

    log_connection(who) << uid() << " adopts view " << unparse_view_state() << "\n";
}

Json Vrreplica::view_payload(const String& peer_uid, const String& why) {
    Json payload = Json::object("viewno", next_view_.viewno.value(),
                                "members", next_view_.members_json(),
                                "primary", next_view_.primary_index,
                                "ackno", commitno_.value());
    if (why)
        payload.set("why", why);
    if (next_view_.group_name())
        payload.set("group_name", next_view_.group_name());
    if (auto next_peer = next_view_.find_pointer(peer_uid))
        if (next_peer->prepared())
            payload["ack"] = true;
    if (cur_view_.nprepared > cur_view_.f()
        && next_view_.nprepared > next_view_.f()
        && next_view_.viewno != cur_view_.viewno
        && !next_view_.me_primary()
        && next_view_.primary().has_ackno()
        && peer_uid == next_view_.primary().uid) {
        payload["confirm"] = true;
        lognumber_t logno = std::max(log_.first(),
                                     next_view_.primary().ackno());
        payload["logno"] = logno.value();
        Json log = Json::array();
        for (; logno < last_logno(); ++logno) {
            auto& li = log_[logno];
            log.push_back_list(li.viewno.value(),
                               li.client_uid,
                               li.client_seqno,
                               li.request);
        }
        payload["log"] = std::move(log);
    }
    return payload;
}

tamed void Vrreplica::send_peer(String peer_uid, Json msg) {
    tamed { Vrchannel* ep = nullptr; }
    while (!(ep = channels_[peer_uid].c))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        ep->send(msg);
}

void Vrreplica::send_view(Vrchannel* who, const String& why, Json payload) {
    if (!payload.get("members"))
        payload.merge(view_payload(who->remote_uid(), why));
    Json msg = Json::array(Vrchannel::m_view, Json(), payload);
    who->send(msg);
    //log_send(who) << msg << " " << unparse_view_state() << "\n";
}

tamed void Vrreplica::send_view(String peer_uid, String why) {
    tamed { Json payload; Vrchannel* ep; }
    payload = view_payload(peer_uid, why);
    while (!(ep = channels_[peer_uid].c))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        send_view(ep, String(), payload);
}

void Vrreplica::broadcast_view(const String& why) {
    for (auto it = next_view_.members.begin();
         it != next_view_.members.end(); ++it)
        if (&next_view_.primary() == &*it
            || !it->prepared())
            send_view(it->uid, why);
}

void Vrreplica::process_join(Vrchannel* who, const Json&) {
    Vrview v;
    if (!next_view_.count(who->remote_uid())) {
        next_view_.add(who->remote_uid(), uid());
        start_view_change();
    }
}

void Vrreplica::initialize_next_view() {
    cur_view_.clear_preparation(false);
    between_views_ = between_views_primary_sent_ = false;
    next_log_.clear();
    Json my_msg = Json::object("ackno", ackno_.value());
    cur_view_.prepare(uid(), my_msg, false);
    next_view_.prepare(uid(), my_msg, true);
}

tamed void Vrreplica::start_view_change() {
    tamed { viewnumber_t view = next_view_.viewno; }
    initialize_next_view();
    broadcast_view(view_why("start"));

    // kick off another view change if this one appears to fail
    twait { tamer::at_delay(k_.view_change_timeout * (1 + rand01() / 8),
                            make_event()); }
    if (cur_view_.viewno < view) {
        logger() << tamer::recent() << ":" << uid() << ": timing out view "
                 << unparse_view_state() << "\n";
        next_view_.advance();
        start_view_change();
    }
}

void Vrreplica::process_request(Vrchannel* who, Json& msg) {
    bool retransmit = msg[2].is_b() && msg[2];
    int seqno_offset = msg[2].is_b() ? 3 : 2;
    if (msg.size() <= seqno_offset || !msg[seqno_offset].is_u()) {
        who->send(Json::array(Vrchannel::m_error, msg[1], false));
        return;
    } else if (!is_primary() || between_views_) {
        send_view(who, view_why("old request"));
        return;
    }

    // add request to our log
    String client_uid = who->remote_uid();
    unsigned client_seqno = msg[seqno_offset].to_u();
    lognumber_t from_storeno = last_logno();
    Json response;
    for (int i = seqno_offset + 1; i != msg.size(); ++i, ++client_seqno)
        if (!retransmit
            || !check_retransmitted_request(client_uid, client_seqno, response))
            log_.emplace_back(cur_view_.viewno, client_uid, client_seqno,
                              std::move(msg[i]));
    process_at_number(from_storeno, at_store_);
    // our log is valid to its end
    ackno_ = sackno_ = last_logno();
    cur_view_.primary().set_ackno(ackno_);

    // broadcast request to backups
    Json commit_msg = commit_log_message(from_storeno, last_logno());
    for (auto it = cur_view_.members.begin();
         it != cur_view_.members.end(); ++it)
        if (!it->has_ackno()
            || it->ackno() == from_storeno
            || tamer::drecent() <=
                 it->ackno_changed_at() + k_.retransmit_log_timeout)
            send_peer(it->uid, commit_msg);
        else
            send_commit_log(&*it, it->ackno(), last_logno());
    commit_sent_at_ = tamer::drecent();

    // perhaps there is a response to a retransmitted request
    if (response) {
        log_send(who) << response << "\n";
        who->send(std::move(response));
    }
}

bool Vrreplica::check_retransmitted_request(const String& client_uid,
                                            unsigned client_seqno,
                                            Json& response) const {
    for (auto i = first_logno(); i != last_logno(); ++i) {
        const Vrlogitem& li = log_[i];
        if (li.client_uid == client_uid
            && li.client_seqno == client_seqno) {
            if (i < commitno_) {
                if (!response)
                    response = Json::array(Vrchannel::m_response, Json::null);
                response.push_back_list(client_seqno, li.response);
            }
            return true;
        }
    }
    return false;
}

Json Vrreplica::commit_log_message(lognumber_t first, lognumber_t last) const {
    Json msg = Json::array(Vrchannel::m_commit,
                           Json::null,
                           cur_view_.viewno.value(),
                           commitno_.value(),
                           commitno_ - decideno_);
    first = std::max(first, log_.first());
    if (first < last) {
        msg.reserve(msg.size() + 1 + (last - first) * 4);
        msg.push_back(first.value());
        for (lognumber_t i = first; i != last; ++i) {
            const Vrlogitem& li = log_[i];
            msg.push_back_list(cur_view_.viewno - li.viewno,
                               li.client_uid, li.client_seqno, li.request);
        }
    }
    return msg;
}

void Vrreplica::send_commit_log(Vrview::member_type* peer,
                                lognumber_t first, lognumber_t last) {
    if (peer->has_ackno() && peer->ackno() < first)
        first = peer->ackno();
    send_peer(peer->uid, commit_log_message(first, last));
}

void Vrreplica::process_commit(Vrchannel* who, Json& msg) {
    if (msg.size() < 5
        || (msg.size() > 5 && (msg.size() - 6) % 4 != 0)
        || !msg[2].is_u()
        || !msg[3].is_u()
        || (msg.size() > 4 && !msg[4].is_u())) {
        who->send(Json::array(Vrchannel::m_error, msg[1], false));
        return;
    }

    viewnumber_t view(msg[2].to_u());
    if (view != cur_view_.viewno || between_views_) {
        if (view == next_view_.viewno
            && cur_view_.nprepared > cur_view_.f()
            && next_view_.nprepared > next_view_.f()) {
            // after confirm is sent, a commit acts to change the view.
            // Normally we wait to go `between_views_` until we've heard
            // from the primary, but this is a message from the primary.
            assert(!next_view_.me_primary()
                   && next_view_.primary().uid == who->remote_uid());
            cur_view_ = next_view_;
            between_views_ = between_views_primary_sent_ = false;
            process_at_number(cur_view_.viewno, at_view_);
            // acknowledge `commitno_` until log confirmed
            ackno_ = sackno_ = commitno_;
            backup_keepalive_loop();
        } else if (view == next_view_.viewno) {
            // couldn't complete view change because we haven't heard from
            // other members of the view; broadcast view to collect
            // acknowledgements
            broadcast_view(view_why("early commit"));
            return;
        } else {
            // odd view
            send_view(who, view_why("old commit"));
            return;
        }
    }
    primary_received_at_ = tamer::drecent();

    lognumber_t old_ackno = ackno_;
    if (msg.size() >= 10)
        process_commit_log(msg);

    lognumber_t commitno = msg[3].to_u();
    lognumber_t decideno = commitno - msg[4].to_u();

    while (ackno_ < last_logno()
           && !log_[ackno_].empty()
           && (ackno_ < decideno
               || log_[ackno_].viewno == cur_view_.viewno))
        ++ackno_;

    sackno_ = std::max(sackno_, ackno_);

    lognumber_t x = std::min(std::max(commitno, commitno_), ackno_);
    if (x != commitno_)
        update_commitno(x);

    x = std::min(std::max(decideno, decideno_), commitno_);
    if (x != decideno_)
        update_decideno(x);

    // XXX send delayed/periodic acks even if there's nothing to acknowledge
    if (msg.size() > 6          /* new data to acknowledge */
        || ackno_ != old_ackno  /* ackno_ changed */
        || decideno > last_logno()) /* we recently came up and need logs */
        send_ack(who);
}

void Vrreplica::process_commit_log(Json& msg) {
    lognumber_t logno = msg[5].to_u();
    size_t nlog = (msg.size() - 6) / 4;

    // create log gap if necessary, but nothing huge
    if (logno > last_logno() + 2048)
        return;
    while (logno > last_logno())
        log_.push_back(Vrlogitem(cur_view_.viewno - 1, String(), 0, Json()));

    // apply log items
    Json* logdata = msg.array_data() + 6;
    for (lognumber_t i = logno; i != logno + nlog; logdata += 4, ++i)
        if (i >= log_.first()) {
            Vrlogitem li(cur_view_.viewno - logdata[0].to_u(),
                         logdata[1].to_s(), logdata[2].to_u(),
                         std::move(logdata[3]));
            if (i == log_.last())
                log_.push_back(std::move(li));
            else if (log_[i].empty()
                     || log_[i].viewno < cur_view_.viewno)
                log_[i] = std::move(li);
        }

    // adjust ackno_ and sackno_
    if (logno <= ackno_)
        ackno_ = std::max(ackno_, logno + nlog);
    else if (sackno_ == ackno_)
        sackno_ = logno;
    else
        sackno_ = std::min(sackno_, logno);

    process_at_number(last_logno(), at_store_);
}

void Vrreplica::send_ack(Vrchannel* primary) {
    primary->send(Json::array(Vrchannel::m_ack,
                              Json::null,
                              cur_view_.viewno.value(),
                              ackno_.value(),
                              sackno_ - ackno_));
}

void Vrreplica::process_ack(Vrchannel* who, const Json& msg) {
    Vrview::member_type* peer;
    if (msg.size() < 4
        || !msg[2].is_u()
        || !msg[3].is_u()) {
        who->send(Json::array(Vrchannel::m_error, msg[1], false));
        return;
    } else if (msg[2].to_u() != cur_view_.viewno
               || between_views_
               || !(peer = cur_view_.find_pointer(who->remote_uid()))) {
        send_view(who, view_why("old ack"));
        return;
    }

    // process acknowledgement
    lognumber_t ackno = msg[3].to_u();
    lognumber_t sackno = ackno + msg[4].to_u();
    peer->set_ackno(ackno);

    // update commitno and decideno
    unsigned count = cur_view_.count_acks(ackno);
    if (count > cur_view_.f()
        && ackno > commitno_)
        process_ack_update_commitno(ackno);
    if (count == cur_view_.size()
        && ackno > decideno_)
        update_decideno(ackno);

    // if sack, respond with gap
    if (msg.size() > 4 && ackno != sackno)
        send_commit_log(peer, ackno, sackno);
}

void Vrreplica::update_commitno(lognumber_t new_commitno) {
    assert(commitno_ <= new_commitno && new_commitno <= last_logno());
    while (commitno_ != new_commitno) {
        Vrlogitem& li = log_[commitno_];
        li.response = state_->commit(li.request);
        ++commitno_;
    }
    process_at_number(commitno_, at_commit_);
}

void Vrreplica::process_ack_update_commitno(lognumber_t new_commitno) {
    lognumber_t old_commitno = commitno_;
    update_commitno(new_commitno);

    std::unordered_map<String, Json> messages;
    while (old_commitno != new_commitno) {
        Vrlogitem& li = log_[old_commitno];
        Json& msg = messages[li.client_uid];
        if (!msg)
            msg = Json::array(Vrchannel::m_response, Json::null);
        msg.push_back_list(li.client_seqno, li.response);
        ++old_commitno;
    }

    for (auto it = messages.begin(); it != messages.end(); ++it) {
        if (Vrchannel* ep = channels_[it->first].c) {
            log_send(ep) << it->second << "\n";
            ep->send(std::move(it->second));
        }
    }
}

void Vrreplica::update_decideno(lognumber_t new_decideno) {
    assert(decideno_ <= new_decideno && new_decideno <= commitno_);
    decideno_ = new_decideno;
    while (log_.first() < decideno_ && vrconstants.trim_log)
        log_.pop_front();
}

tamed void Vrreplica::primary_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    while (1) {
        twait { tamer::at_delay(k_.primary_keepalive_timeout / 4,
                                make_event()); }
        if (cur_view_.viewno != view || between_views_)
            break;
        if (tamer::drecent() - commit_sent_at_
              >= k_.primary_keepalive_timeout / 2
            && !stopped_) {
            for (auto it = cur_view_.members.begin();
                 it != cur_view_.members.end(); ++it)
                send_commit_log(&*it, it->ackno(), last_logno());
            commit_sent_at_ = tamer::drecent();
        }
    }
}

tamed void Vrreplica::backup_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    primary_received_at_ = tamer::drecent();
    while (1) {
        twait { tamer::at_delay(k_.primary_keepalive_timeout * (0.375 + rand01() / 8),
                                make_event()); }
        if (next_view_.viewno != view)
            break;
        if (tamer::drecent() - primary_received_at_
              >= k_.primary_keepalive_timeout
            && !stopped_) {
            next_view_.advance();
            start_view_change();
            break;
        }
    }
}

void Vrreplica::stop() {
    stopped_ = true;
}

void Vrreplica::go() {
    stopped_ = false;
}
