// -*- mode: c++ -*-
#include "vrchannel.hh"
#include <assert.h>

const String Vrchannel::m_request("req");
    // seqno, request [, request]*
const String Vrchannel::m_response("res");
    // [seqno, reply]*
const String Vrchannel::m_commit("commit");
    // P->R: [3, xxx, viewno, commitno, decide_delta,
    //        [logno, [view_delta, client_uid, client_seqno, request]*]]
const String Vrchannel::m_ack("ack");
    // R->P: [3, xxx, viewno, storeno]
const String Vrchannel::m_handshake("handshake");
    // [my_uid, your_uid, handshake_value]
const String Vrchannel::m_join("join");
    // []
const String Vrchannel::m_view("view");
    // view_object
const String Vrchannel::m_error("error");


void Vrchannel::connect(String, Json, tamer::event<Vrchannel*>) {
    assert(0);
}

void Vrchannel::receive_connection(tamer::event<Vrchannel*>) {
    assert(0);
}

tamed void Vrchannel::handshake(bool active_end, double message_timeout,
                                double timeout, tamer::event<bool> done) {
    tamed {
        Json msg;
        double start_time = tamer::drecent();
    }

    // handshake loop with retry
    while (1) {
        if (active_end) {
            Json handshake_msg = Json::array(m_handshake, Json::null,
                                             local_uid(), remote_uid(),
                                             connection_uid());
            log_send(this) << handshake_msg << "\n";
            send(handshake_msg);
        }
        twait {
            receive(tamer::add_timeout(message_timeout,
                                       make_event(msg),
                                       Json(false)));
        }
        if (!msg.is_bool() || tamer::drecent() >= start_time + timeout)
            break;
    }

    if (check_handshake(msg)) {
        log_receive(this) << msg << "\n";
        process_handshake(msg, !active_end);
        done(true);
    } else if (!msg) { // null or false
        log_receive(this) << "handshake timeout\n";
        done(false);
    } else {
        log_receive(this) << "bad handshake " << msg << "\n";
        done(false);
    }
}

bool Vrchannel::check_handshake(const Json& msg) const {
    return msg.is_a()
        && msg.size() >= 5
        && msg[0] == m_handshake
        && msg[2].is_s()
        && (remote_uid().empty()
            || msg[2].to_s() == remote_uid())
        && (msg[3].is_null()
            || (msg[3].is_s() && msg[3].to_s().empty())
            || (msg[3].is_s() && msg[3].to_s() == local_uid()))
        && msg[4].is_s()
        && !msg[4].to_s().empty()
        && (connection_uid().empty()
            || msg[4].to_s() == connection_uid());
}

void Vrchannel::process_handshake(const Json& msg, bool reply) {
    assert(check_handshake(msg));
    if (remote_uid_.empty())
        remote_uid_ = msg[2].to_s();
    if (connection_uid_.empty())
        connection_uid_ = msg[4].to_s();
    if (reply)
        send(Json::array(m_handshake, msg[1],
                         local_uid(), remote_uid(), connection_uid()));
}

void Vrchannel::send(Json) {
    assert(0);
}

void Vrchannel::receive(tamer::event<Json>) {
    assert(0);
}

void Vrchannel::close() {
}