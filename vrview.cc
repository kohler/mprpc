#include "vrview.hh"
#include <unordered_map>
#include <tamer/tamer.hh>

Vrconstants vrconstants;

Vrview::Vrview()
    : viewno(0), primary_index(0), my_index(-1), nprepared(0), nconfirmed(0) {
}

Vrview Vrview::make_singular(String group_name, String peer_uid) {
    Vrview v;
    v.group_name_ = std::move(group_name);
    v.members.push_back(member_type(std::move(peer_uid), Json()));
    v.primary_index = v.my_index = 0;
    v.primary().set_ackno(0);
    return v;
}

Json Vrview::clean_peer_name(Json peer_name) {
    if (!peer_name.is_o()
        || peer_name.empty()
        || (peer_name.size() == 1 && peer_name.count("uid")))
        return Json();
    else
        return std::move(peer_name);
}

bool Vrview::assign_parse(Json msg, bool require_view, const String& my_uid) {
    group_name_ = String();
    viewno = 0;
    primary_index = my_index = -1;
    members.clear();
    nprepared = nconfirmed = 0;

    if (!msg.is_o())
        return false;

    if (msg["group_name"].is_s())
        group_name_ = msg["group_name"].to_s();
    else if (!msg["group_name"].is_null())
        return false;

    Json membersj = msg["members"];
    if (!(membersj.is_a() || membersj.is_o()))
        return false;

    Json viewnoj = msg["viewno"];
    if (viewnoj.is_posint())
        viewno = viewnoj.to_u();
    else if (viewnoj.is_null() && !require_view)
        viewno = 0;
    else
        return false;

    Json primaryj = msg["primary"];
    String primary_name;
    if (primaryj.is_posint())
        primary_index = primaryj.to_i();
    else if (primaryj.is_s() && !membersj.is_o()) {
        primary_index = -1;
        primary_name = primaryj.to_s();
    } else if (primaryj.is_null() && !require_view)
        primary_index = -1;
    else
        return false;

    std::unordered_map<String, int> seen_uids;
    int itindex = 0;
    for (auto it = membersj.begin(); it != membersj.end(); ++it, ++itindex) {
        // ugh, want to support the following formats for members:
        // ["uid", {"uid": "foo"}] and {"uid": true}
        // requires hoop jumping
        Json peer_name = it->second;
        if (peer_name.is_string())
            peer_name = Json::object("uid", std::move(peer_name));
        else if (peer_name.is_b())
            peer_name = Json();
        if (peer_name && !peer_name.is_o())
            return false;

        String peer_uid;
        if (membersj.is_o())
            peer_uid = it->first;
        else if (peer_name.is_o() && peer_name["uid"].is_s())
            peer_uid = peer_name["uid"].to_s();
        if (peer_uid.empty()
            || (peer_name && peer_name["uid"] && peer_name["uid"] != peer_uid)
            || seen_uids.count(peer_uid))
            return false;

        members.push_back(member_type(std::move(peer_uid),
                                      clean_peer_name(std::move(peer_name))));

        if (peer_uid == my_uid)
            my_index = itindex;
        if (primary_name && peer_uid == primary_name)
            primary_index = itindex;
        seen_uids[peer_uid] = 1;
    }

    if ((primary_index < 0 && (require_view || primary_name))
        || primary_index >= (int) members.size())
        return false;
    return true;
}

int Vrview::compare(const Vrview& x) const {
    if (viewno != x.viewno)
        return viewno < x.viewno ? -1 : 1;
    if (members.size() != x.members.size())
        return members.size() < x.members.size() ? -1 : 1;
    if (primary_index != x.primary_index)
        return primary_index < x.primary_index ? -1 : 1;
    for (size_t i = 0; i != members.size(); ++i)
        if (int cmp = members[i].uid.compare(x.members[i].uid))
            return cmp;
    return 0;
}

bool Vrview::shared_quorum(const Vrview& x) const {
    size_t nshared = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (x.count(it->uid))
            ++nshared;
    return nshared == size()
        || nshared == x.size()
        || (nshared > f() && nshared > x.f());
}

void Vrview::prepare(String uid, const Json& payload, bool is_next) {
    if (auto it = find_pointer(uid)) {
        if (!it->prepared_) {
            it->prepared_ = true;
            ++nprepared;
        }
        if (payload["confirm"] && !it->confirmed_) {
            it->confirmed_ = true;
            ++nconfirmed;
        }
        if (!payload["ackno"].is_null() && is_next)
            it->set_ackno(payload["ackno"].to_u());
    }
}

void Vrview::set_matching_logno(String uid, lognumber_t logno) {
    if (auto it = find_pointer(uid)) {
        it->has_matching_logno_ = true;
        it->matching_logno_ = logno;
    }
}

void Vrview::reduce_matching_logno(lognumber_t logno) {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->has_matching_logno_
            && logno < it->matching_logno_)
            it->matching_logno_ = logno;
}

void Vrview::clear_preparation(bool is_next) {
    nprepared = nconfirmed = 0;
    for (auto& it : members)
        it.prepared_ = it.confirmed_ = false;
    if (is_next)
        for (auto& it : members)
            it.has_ackno_ = it.has_matching_logno_ = false;
}

void Vrview::add(String peer_uid, const String& my_uid) {
    auto it = members.begin();
    while (it != members.end() && it->uid < peer_uid)
        ++it;
    if (it == members.end() || it->uid != peer_uid)
        members.insert(it, member_type(std::move(peer_uid), Json()));

    my_index = -1;
    for (size_t i = 0; i != members.size(); ++i)
        if (members[i].uid == my_uid)
            my_index = i;

    advance();
}

void Vrview::advance() {
    clear_preparation(true);
    ++viewno;
    if (!viewno)
        ++viewno;
    primary_index = viewno % members.size();
}

Json Vrview::acks_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it) {
        Json x = Json::array(it->uid);
        if (it->has_ackno_)
            x.push_back(it->ackno_.value());
        bool is_primary = it - members.begin() == primary_index;
        bool is_me = it - members.begin() == my_index;
        if (is_primary || is_me)
            x.push_back(String(is_primary ? "p" : "") + String(is_me ? "*" : ""));
        j.push_back(x);
    }
    return j;
}

unsigned Vrview::count_acks(lognumber_t ackno) const {
    unsigned count = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->has_ackno() && it->ackno() >= ackno)
            ++count;
    return count;
}

void Vrview::member_type::set_ackno(lognumber_t ackno) {
    if (!has_ackno_ || ackno_ != ackno) {
        has_ackno_ = true;
        ackno_ = ackno;
        ackno_changed_at_ = tamer::drecent();
    }
}
