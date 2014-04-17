// -*- mode: c++ -*-
#include "vrtest.hh"
#include "vrreplica.hh"
#include "vrclient.hh"
#include "vrstate.hh"
#include <algorithm>
#include "tamer/ref.hh"

class Vrtestnode {
  public:
    inline Vrtestnode(const String& uid, Vrtestcollection* collection);

    inline const String& uid() const {
        return uid_;
    }
    inline Json name() const {
        return Json::object("uid", uid_);
    }
    inline Vrtestlistener* listener() const {
        return listener_;
    }
    inline Vrtestcollection* collection() const {
        return collection_;
    }

    Vrtestchannel* connect(Vrtestnode* x);

  private:
    String uid_;
    Vrtestcollection* collection_;
    Vrtestlistener* listener_;
};

class Vrtestlistener : public Vrchannel {
  public:
    inline Vrtestlistener(String my_uid, Vrtestcollection* collection)
        : Vrchannel(my_uid, String()), collection_(collection) {
        set_connection_uid(my_uid);
    }
    void connect(String peer_uid, Json peer_name,
                 tamer::event<Vrchannel*> done);
    void receive_connection(tamer::event<Vrchannel*> done);

  private:
    Vrtestcollection* collection_;
    tamer::channel<Vrchannel*> listenq_;
    friend class Vrtestnode;
};

class Vrtestchannel : public Vrchannel {
  public:
    Vrtestchannel(Vrtestnode* from, Vrtestnode* to, double loss_p);
    ~Vrtestchannel();
    inline void set_delay(double d);
    inline void set_loss(double p);
    inline Vrtestcollection* collection() const {
        return from_node_->collection();
    }
    void send(Json msg);
    void receive(tamer::event<Json> done);
    void close();

  private:
    Vrtestnode* from_node_;
    double delay_;
    double loss_p_;
    typedef std::pair<double, Json> message_t;
    std::deque<message_t> q_;
    std::deque<tamer::event<Json> > w_;
    Vrtestchannel* peer_;
    tamer::event<> coroutine_;
    tamer::ref ref_;
    tamed void coroutine();
    inline void do_send(Json msg);
    friend class Vrtestnode;
};


Vrtestnode::Vrtestnode(const String& uid, Vrtestcollection* collection)
    : uid_(uid), collection_(collection) {
    listener_ = new Vrtestlistener(uid, collection);
}

Vrtestchannel* Vrtestnode::connect(Vrtestnode* n) {
    assert(n->uid() != uid());
    Vrtestchannel* my = new Vrtestchannel(this, n, collection_->loss_p());
    Vrtestchannel* peer = new Vrtestchannel(n, this, collection_->loss_p());
    my->peer_ = peer;
    peer->peer_ = my;
    n->listener()->listenq_.push_back(peer);
    return my;
}


Vrtestchannel::Vrtestchannel(Vrtestnode* from, Vrtestnode* to, double loss_p)
    : Vrchannel(from->uid(), to->uid()), from_node_(from),
      delay_(0.05 + 0.0125 * from->collection()->rand01()), loss_p_(loss_p) {
    assert(loss_p_ >= 0 && loss_p_ <= 1);
    coroutine();
}

Vrtestchannel::~Vrtestchannel() {
    close();
    while (!w_.empty()) {
        w_.front().unblock();
        w_.pop_front();
    }
    q_.clear();
}

void Vrtestchannel::close() {
    coroutine_();
    if (peer_) {
        peer_->do_send(Json());
        peer_->peer_ = 0;
    }
    peer_ = 0;
}

void Vrtestchannel::set_delay(double d) {
    delay_ = d;
}

void Vrtestchannel::set_loss(double p) {
    assert(p >= 0 && p <= 1);
    loss_p_ = p;
}

inline void Vrtestchannel::do_send(Json msg) {
    while (!w_.empty() && !w_.front())
        w_.pop_front();
    if (!w_.empty()
        && q_.empty()
        && delay_ <= 0) {
        w_.front()(std::move(msg));
        w_.pop_front();
    } else {
        q_.push_back(std::make_pair(tamer::drecent() + delay_,
                                    std::move(msg)));
        if (!w_.empty())
            coroutine_();
    }
}

void Vrtestchannel::send(Json msg) {
    if ((!loss_p_ || collection()->rand01() >= loss_p_) && peer_)
        peer_->do_send(std::move(msg));
}

void Vrtestchannel::receive(event<Json> done) {
    double now = tamer::drecent();
    if (w_.empty()
        && !q_.empty()
        && q_.front().first <= now) {
        done(std::move(q_.front().second));
        q_.pop_front();
    } else if (peer_) {
        w_.push_back(std::move(done));
        if (!q_.empty())
            coroutine_();
    } else
        done(Json());
}

tamed void Vrtestchannel::coroutine() {
    tamed { tamer::ref_monitor mon(ref_); }
    while (mon) {
        while (!w_.empty() && !w_.front())
            w_.pop_front();
        if (!w_.empty() && !q_.empty()
            && tamer::drecent() >= q_.front().first) {
            w_.front()(std::move(q_.front().second));
            w_.pop_front();
            q_.pop_front();
        } else if (!w_.empty() && !q_.empty())
            twait { tamer::at_time(q_.front().first, make_event()); }
        else
            twait { coroutine_ = make_event(); }
    }
}


void Vrtestlistener::connect(String peer_uid, Json, event<Vrchannel*> done) {
    if (Vrtestnode* n = collection_->test_node(peer_uid))
        done(collection_->test_node(local_uid())->connect(n));
    else
        done(nullptr);
}

void Vrtestlistener::receive_connection(event<Vrchannel*> done) {
    listenq_.pop_front(done);
}


Vrtestcollection::Vrtestcollection(unsigned seed, double loss_p)
    : state_(new Vrstate), rg_(seed), loss_p_(loss_p),
      decideno_(0), commitno_(0) {
}

Vrreplica* Vrtestcollection::add_replica(const String& uid) {
    assert(testnodes_.find(uid) == testnodes_.end());
    Vrtestnode* tn = new Vrtestnode(uid, this);
    testnodes_[uid] = tn;
    Vrreplica* r = new Vrreplica(tn->uid(), state_, tn->listener(), Json(), rg_);
    replica_map_[uid] = r;
    replicas_.push_back(r);
    std::sort(replicas_.begin(), replicas_.end(), [](Vrreplica* a, Vrreplica* b) {
            return a->uid() < b->uid();
        });
    return r;
}

Json Vrtestcollection::replica_configuration() const {
    Json membersj = Json::object();
    for (auto r : replicas_)
        membersj[r->uid()] = Json::object();
    return Json::object("members", std::move(membersj));
}

Vrclient* Vrtestcollection::add_client(const String& uid) {
    assert(testnodes_.find(uid) == testnodes_.end());
    Vrtestnode* tn = new Vrtestnode(uid, this);
    testnodes_[uid] = tn;
    return new Vrclient(tn->listener(), replica_configuration(), rg_);
}

void Vrtestcollection::print_lognos() const {
    const char* sep = "  ";
    for (auto r : replicas_) {
        std::cerr << sep << r->uid() << ":" << r->first_logno() << ":";
        if (r->decideno() != r->first_logno())
            std::cerr << r->decideno();
        std::cerr << ":";
        if (r->commitno() != r->decideno())
            std::cerr << r->commitno();
        std::cerr << ":";
        if (r->last_logno() != r->commitno())
            std::cerr << r->last_logno();
        std::cerr << "(" << r->current_view().acks_json() << ")";
        sep = ", ";
    }
    std::cerr << "\n";
}

void Vrtestcollection::print_log_position(lognumber_t l) const {
    std::cerr << "  l#" << l << "<";
    const char* sep = "";
    for (auto r : replicas_)
        if (l < r->last_logno()) {
            std::cerr << sep << r->uid() << ":";
            if (l < r->first_logno())
                std::cerr << "trunc";
            else
                std::cerr << r->log_entry(l);
            sep = ", ";
        }
    std::cerr << ">\n";
}

void Vrtestcollection::check() {
    unsigned f = this->f();

    // calculate actual commit numbers
    std::vector<lognumber_t> first_lognos, last_lognos;
    lognumber_t max_decideno, max_commitno;
    for (auto r : replicas_) {
        first_lognos.push_back(r->first_logno());
        last_lognos.push_back(r->last_logno());
        if (r == replicas_.front()) {
            max_decideno = r->decideno();
            max_commitno = r->commitno();
        } else {
            max_decideno = std::max(max_decideno, r->decideno());
            max_commitno = std::max(max_commitno, r->commitno());
        }
    }
    std::sort(first_lognos.begin(), first_lognos.end());
    lognumber_t first_logno = first_lognos[f];
    std::sort(last_lognos.begin(), last_lognos.end());
    lognumber_t last_logno = last_lognos.back();

    // commit never goes backwards
    assert(decideno_ <= max_decideno);
    decideno_ = max_decideno;
    assert(first_logno <= decideno_);

    // advance commit number (check replication)
    std::vector<unsigned> commitmap;
    std::vector<const Vrlogitem*> itemmap;
    while (1) {
        itemmap.assign(replicas_.size(), nullptr);
        commitmap.assign(replicas_.size(), 1);
        for (size_t i = 0; i != replicas_.size(); ++i) {
            Vrreplica* r = replicas_[i];
            if (commitno_ >= r->first_logno()
                && commitno_ < r->last_logno()
                && r->log_entry(commitno_).is_real())
                itemmap[i] = &r->log_entry(commitno_);
        }
        for (size_t i = 1; i != replicas_.size(); ++i)
            if (itemmap[i])
                for (size_t j = 0; j != i; ++j)
                    if (itemmap[j]
                        && itemmap[i]->viewno == itemmap[j]->viewno) {
                        ++commitmap[j];
                        break;
                    }
        auto maxindex = std::max_element(commitmap.begin(), commitmap.end()) - commitmap.begin();
        if (commitmap[maxindex] <= f)
            break;
        committed_log_.push_back(replicas_[maxindex]->log_entry(commitno_));
        ++commitno_;
    }
    // no one is allowed to think more has committed than has actually committed
    assert(max_commitno <= commitno_);

    // count # committed
    Vrlog<unsigned, lognumber_t::value_type>
        commit_counts(first_logno, last_logno, 0);

    // check integrity of log
    for (auto r : replicas_) {
        assert(commitno_ >= r->commitno());
        assert(max_decideno >= r->decideno());
        if (r->first_logno() > r->decideno()
            || r->decideno() > r->commitno()
            || r->commitno() > r->last_logno()
            || r->decideno() > r->ackno()
            || r->ackno() > r->sackno()
            || r->sackno() > r->last_logno())
            std::cerr << "check: " << r->uid() << " bad commits "
                      << r->first_logno() << ":" << r->decideno()
                      << ":" << r->commitno() << ":" << r->last_logno()
                      << " ack " << r->ackno()
                      << " sack " << r->sackno() << "\n";
        assert(r->first_logno() <= r->decideno());
        assert(r->decideno() <= r->commitno());
        assert(r->commitno() <= r->last_logno());
        assert(r->decideno() <= r->ackno());
        assert(r->ackno() <= r->sackno());
        assert(r->sackno() <= r->last_logno());
        lognumber_t first = std::max(first_logno, r->first_logno());
        lognumber_t last = std::min(commitno_, r->last_logno());
        for (; first < last; ++first) {
            const Vrlogitem& li = r->log_entry(first);
            const Vrlogitem& cli = committed_log_[first];
            if (li.is_real()) {
                assert(cli.viewno != li.viewno || cli.request_equals(li));
                if (first < commit_counts.last()
                    && cli.viewno == li.viewno)
                    ++commit_counts[first];
            }
        }
    }

    // Every "decided" log element has all commits
    unsigned truncatepos = 0, missingpos = 0;
    for (lognumber_t l = first_logno; l != max_decideno; ++l) {
        while (truncatepos != size() && first_lognos[truncatepos] <= l)
            ++truncatepos;
        while (missingpos != size() && last_lognos[missingpos] <= l)
            ++missingpos;
        if (commit_counts[l] != truncatepos - missingpos) {
            std::cerr << "check: decided l#" << l << "<" << committed_log_[l] << "> replicated only " << commit_counts[l] << " times (want " << (truncatepos - missingpos) << ")\n";
            print_lognos();
            print_log_position(l);
        }
        assert(commit_counts[l] == truncatepos - missingpos);
    }
    // Every "committed" log element has >= f + 1 commits
    for (lognumber_t l = max_decideno; l != commitno_; ++l) {
        if (commit_counts[l] < f + 1) {
            std::cerr << "check: committed l#" << l << "<" << committed_log_[l] << "> replicated only " << commit_counts[l] << " times\n";
            print_lognos();
            print_log_position(l);
        }
        assert(commit_counts[l] >= f + 1);
    }
}
