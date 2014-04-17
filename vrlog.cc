#include "vrlog.hh"

std::ostream& operator<<(std::ostream& str, const Vrlogitem& x) {
    if (!x.empty())
        return str << x.request << "@" << x.viewno;
    else
        return str << "~empty~";
}
