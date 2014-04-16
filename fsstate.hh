#ifndef FSSTATE_HH
#define FSSTATE_HH
#include "vrstate.hh"

class Fsstate : public Vrstate {
  public:
    Fsstate()
        : fs_(Json::object()) {
    }

    Json commit(Json req);

  private:
    Json fs_;
};

#endif
