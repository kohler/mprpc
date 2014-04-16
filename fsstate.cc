#include "fsstate.hh"

Json Fsstate::commit(Json req) {
    if (!req.is_a() || !req[0].is_s() || !req[1].is_s())
        return Json();
    if (req[0] == "read")
        return fs_[req[1].to_s()];
    else if (req[0] == "write") {
        fs_[req[1].to_s()] = req[2].to_s();
        return Json(true);
    } else
        return Json();
}
