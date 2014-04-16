#include "vrview.hh"
#include <unordered_map>
#include <tamer/tamer.hh>

Vrconstants vrconstants;

Vrview::Vrview()
    : viewno(0), primary_index(0), my_index(-1), nacked(0), nconfirmed(0) {
}

Vrview Vrview::make_singular(String peer_uid, Json peer_name) {
    Vrview v;
    v.members.push_back(member_type(std::move(peer_uid),
                                    std::move(peer_name)));
    v.primary_index = v.my_index = 0;
    v.account_ack(&v.members.back(), 0);
    return v;
}

bool Vrview::assign(Json msg, const String& my_uid) {
    if (!msg.is_o())
        return false;
    Json viewnoj = msg["viewno"];
    Json membersj = msg["members"];
    Json primaryj = msg["primary"];
    if (!(viewnoj.is_i() && viewnoj.to_i() >= 0
          && membersj.is_a()
          && primaryj.is_i()
          && primaryj.to_i() >= 0 && primaryj.to_i() < membersj.size()))
        return false;

    viewno = viewnoj.to_u64();
    primary_index = primaryj.to_i();
    my_index = -1;

    std::unordered_map<String, int> seen_uids;
    String uid;
    for (auto it = membersj.abegin(); it != membersj.aend(); ++it) {
        Json peer_name;
        if (it->is_object())
            peer_name = *it;
        else if (it->is_string())
            peer_name = Json::object("uid", *it);
        if (!peer_name.is_object()
            || !peer_name.get("uid").is_string()
            || !(uid = peer_name.get("uid").to_s())
            || seen_uids.find(uid) != seen_uids.end())
            return false;
        seen_uids[uid] = 1;
        if (uid == my_uid)
            my_index = it - membersj.abegin();
        members.push_back(member_type(uid, std::move(peer_name)));
    }

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
        if (!it->acked) {
            it->acked = true;
            ++nacked;
        }
        if (payload["confirm"] && !it->confirmed) {
            it->confirmed = true;
            ++nconfirmed;
        }
        if (!payload["ackno"].is_null() && is_next)
            account_ack(it, payload["ackno"].to_u());
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
    nacked = nconfirmed = 0;
    for (auto& it : members)
        it.acked = it.confirmed = false;
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
            x.push_back_list(it->ackno_.value(), it->ackno_count_);
        bool is_primary = it - members.begin() == primary_index;
        bool is_me = it - members.begin() == my_index;
        if (is_primary || is_me)
            x.push_back(String(is_primary ? "p" : "") + String(is_me ? "*" : ""));
        j.push_back(x);
    }
    return j;
}

void Vrview::account_ack(member_type* peer, lognumber_t ackno) {
    bool has_old_ackno = peer->has_ackno();
    lognumber_t old_ackno = peer->ackno();
    if (!has_old_ackno || old_ackno <= ackno) {
        peer->has_ackno_ = true;
        peer->ackno_ = ackno;
        peer->ackno_count_ = 0;
        if (!has_old_ackno || old_ackno != ackno)
            peer->ackno_changed_at_ = tamer::drecent();
        for (auto it = members.begin(); it != members.end(); ++it)
            if (it->has_ackno_) {
                if (it->ackno_ <= ackno
                    && (!has_old_ackno || it->ackno_ > old_ackno)
                    && &*it != peer)
                    ++it->ackno_count_;
                if (ackno <= it->ackno_)
                    ++peer->ackno_count_;
            }
    }
}

bool Vrview::account_all_acks() {
    bool changed = false;
    //Json cj = acks_json();
    for (auto it = members.begin(); it != members.end(); ++it) {
        unsigned old_ackno_count = it->ackno_count_;
        it->ackno_count_ = 0;
        for (auto jt = members.begin(); jt != members.end(); ++jt)
            if (it->has_ackno_ && jt->has_ackno_
                && it->ackno_ <= jt->ackno_)
                ++it->ackno_count_;
        changed = changed || it->ackno_count_ != old_ackno_count;
    }
    //std::cerr << (changed ? "! " : ". ") << " => " << cj << " => " << acks_json() << "\n";
    return changed;
}
