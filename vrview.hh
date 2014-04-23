#ifndef VRVIEW_HH
#define VRVIEW_HH 1
#include "vrlog.hh"

struct Vrview {
    struct member_type {
        String uid;
        Json peer_name;

        explicit member_type(String peer_uid, Json peer_name)
            : uid(std::move(peer_uid)), peer_name(std::move(peer_name)),
              prepared_(false), confirmed_(false),
              has_ackno_(false), has_matching_logno_(false) {
            if (this->peer_name.is_o() && this->peer_name.empty())
                this->peer_name = Json();
            if (this->peer_name.is_o() && !this->peer_name["uid"])
                this->peer_name["uid"] = uid;
            if (this->peer_name.is_o() && this->peer_name["uid"])
                assert(this->peer_name["uid"] == uid);
        }

        bool prepared() const {
            return prepared_;
        }
        bool confirmed() const {
            return confirmed_;
        }
        bool has_ackno() const {
            return has_ackno_;
        }
        lognumber_t ackno() const {
            return ackno_;
        }
        double ackno_changed_at() const {
            return ackno_changed_at_;
        }
        void set_ackno(lognumber_t ackno);

        bool has_matching_logno() const {
            return has_matching_logno_;
        }
        lognumber_t matching_logno() const {
            assert(has_matching_logno());
            return matching_logno_;
        }

      private:
        bool prepared_;
        bool confirmed_;
        bool has_ackno_;
        bool has_matching_logno_;
        lognumber_t ackno_;
        lognumber_t matching_logno_;
        double ackno_changed_at_;

        friend struct Vrview;
    };

    viewnumber_t viewno;
    std::vector<member_type> members;
    int primary_index;
    int my_index;
    unsigned nprepared;
    unsigned nconfirmed;

    Vrview();
    static Vrview make_singular(String group_name, String peer_uid);

    inline bool empty() const {
        return members.empty();
    }
    inline unsigned size() const {
        return members.size();
    }
    inline unsigned f() const {
        return size() / 2;
    }
    inline String uid() const {
        assert(my_index >= 0);
        return members[my_index].uid;
    }
    inline bool me_primary() const {
        return primary_index == my_index;
    }
    inline member_type& primary() {
        return members[primary_index];
    }
    inline const String& group_name() const {
        return group_name_;
    }
    inline std::vector<member_type>::const_iterator begin() const {
        return members.begin();
    }
    inline std::vector<member_type>::const_iterator end() const {
        return members.end();
    }
    inline std::vector<member_type>::iterator begin() {
        return members.begin();
    }
    inline std::vector<member_type>::iterator end() {
        return members.end();
    }

    inline int count(const String& uid) const;
    inline member_type* find_pointer(const String& uid);
    inline const member_type* find_pointer(const String& uid) const;

    Json members_json() const;
    Json acks_json() const;

    static Json clean_peer_name(Json peer_name);
    bool assign_parse(Json msg, bool require_view, const String& my_uid);
    void add(String uid, const String& my_uid);
    void advance();

    int compare(const Vrview& x) const;
    bool shared_quorum(const Vrview& x) const;

    void clear_preparation(bool is_next);
    void prepare(String uid, const Json& payload, bool is_next);
    void set_matching_logno(String uid, lognumber_t logno);
    void reduce_matching_logno(lognumber_t logno);

    unsigned count_acks(lognumber_t ackno) const;

  private:
    String group_name_;
};

inline int Vrview::count(const String& uid) const {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return 1;
    return 0;
}

inline Vrview::member_type* Vrview::find_pointer(const String& uid) {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return &*it;
    return nullptr;
}

inline const Vrview::member_type* Vrview::find_pointer(const String& uid) const {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return &*it;
    return nullptr;
}

inline Json Vrview::members_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it)
        if (!it->peer_name)
            j.push_back(it->uid);
        else
            j.push_back(it->peer_name);
    return j;
}

inline bool operator==(const Vrview& a, const Vrview& b) {
    return a.compare(b) == 0;
}

inline bool operator!=(const Vrview& a, const Vrview& b) {
    return a.compare(b) != 0;
}


class Vrconstants {
  public:
    double message_timeout;
    double client_message_timeout;
    double request_timeout;
    double handshake_timeout;
    double primary_keepalive_timeout;
    double backup_keepalive_timeout;
    double view_change_timeout;
    double retransmit_log_timeout;
    bool trim_log;

    Vrconstants()
        : message_timeout(0.5),
          client_message_timeout(1.5),
          request_timeout(10),
          handshake_timeout(5),
          primary_keepalive_timeout(1),
          backup_keepalive_timeout(2),
          view_change_timeout(0.5),
          retransmit_log_timeout(2),
          trim_log(true) {
    }
};

extern Vrconstants vrconstants;

#endif
