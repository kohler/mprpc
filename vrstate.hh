#ifndef VRSTATE_HH
#define VRSTATE_HH 1
#include "json.hh"

class Vrstate {
  public:
    inline Vrstate() {
    }
    virtual ~Vrstate() {
    }

    virtual Json commit(Json req) {
        return req;
    }
};

#endif
