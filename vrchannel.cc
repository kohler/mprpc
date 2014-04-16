// -*- mode: c++ -*-
#include "vrchannel.hh"
#include <assert.h>

Json Vrchannel::local_name() const {
    return Json::object("uid", local_uid());
}

Json Vrchannel::remote_name() const {
    return Json::object("uid", remote_uid());
}

void Vrchannel::connect(String, Json, tamer::event<Vrchannel*>) {
    assert(0);
}

void Vrchannel::receive_connection(tamer::event<Vrchannel*>) {
    assert(0);
}

void Vrchannel::send(Json) {
    assert(0);
}

void Vrchannel::receive(tamer::event<Json>) {
    assert(0);
}

void Vrchannel::close() {
}
