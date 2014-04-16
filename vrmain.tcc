// -*- mode: c++ -*-
#include "vrtest.hh"
#include "vrnetchannel.hh"
#include "vrreplica.hh"
#include "clp.h"

Logger logger(std::cout);

static String make_replica_uid() {
    static int counter;
    return String("n") + String(counter++);
}

static String make_client_uid() {
    static int counter;
    return String("c") + String(counter++);
}

tamed void many_requests(Vrclient* client) {
    tamed { int n = 1; }
    while (1) {
        twait { client->request("req" + String(n), make_event()); }
        ++n;
        twait { tamer::at_delay(0.5, make_event()); }
    }
}

tamed void go(Vrtestcollection& vrg, std::vector<Vrreplica*>& nodes) {
    tamed {
        Vrclient* client;
        Json j;
    }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[0]->join(nodes[1]->uid(), make_event()); }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait {
        nodes[0]->at_view(1, make_event());
        nodes[1]->at_view(1, make_event());
    }

    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[2]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(2, make_event());
        nodes[1]->at_view(2, make_event());
        nodes[2]->at_view(2, make_event());
    }

    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[4]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(3, make_event());
        nodes[1]->at_view(3, make_event());
        nodes[2]->at_view(3, make_event());
        nodes[4]->at_view(3, make_event());
    }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);

    client = vrg.add_client(make_client_uid());
    twait { client->connect(nodes[0]->uid(), make_event()); }
    many_requests(client);
    twait { tamer::at_delay_usec(10000, make_event()); }
    twait { tamer::at_delay_sec(3, make_event()); }
    nodes[4]->stop();
    twait { tamer::at_delay_sec(5, make_event()); }
    nodes[4]->go();

    twait { tamer::at_delay_sec(50000, make_event()); }
    exit(0);
}

void run_test(unsigned seed, double loss_p, unsigned n) {
    Vrtestcollection vrg(seed, loss_p);
    std::vector<Vrreplica*> nodes;
    for (unsigned i = 0; i < n; ++i)
        nodes.push_back(vrg.add_replica(make_replica_uid()));

    go(vrg, nodes);

    while (1) {
        tamer::once();
        vrg.check();
    }
}


namespace {
tamed void join_config(Vrreplica* vrr, String uid, Json name) {
    tamed { double backoff = 0.1; }
    while (vrr->current_view().size() == 1) {
        twait { vrr->join(uid, name, make_event()); }
        backoff = std::min(backoff * 2, 10.0);
        twait { tamer::at_delay(backoff, make_event()); }
    }
}

void run_config(Json config, String replicaname) {
    std::mt19937 rg(replicaname.hashcode() + time(0));

    String groupname = config["name"].to_s();
    if (groupname.empty())
        groupname = "vr";

    Json members = config["members"];
    if (replicaname) {
        Json my_name = members[replicaname];
        assert(my_name && my_name["port"].is_u());
        Vrnetlistener* my_conn = new Vrnetlistener(replicaname, my_name["port"].to_u(), rg);
        assert(my_conn->ok());
        Vrreplica* me = new Vrreplica(groupname, new Vrstate, my_conn, my_name, rg);
        for (auto it = members.obegin(); it != members.oend(); ++it)
            if (it->first != replicaname)
                join_config(me, it->first, it->second);
    }

    tamer::loop();
}
}


static Clp_Option options[] = {
    { "f", 'f', 0, Clp_ValUnsigned, 0 },
    { "loss", 'l', 0, Clp_ValDouble, 0 },
    { "n", 'n', 0, Clp_ValUnsigned, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate },
    { "seed", 's', 0, Clp_ValUnsigned, 0 },
    { "config", 'c', 0, Clp_ValString, Clp_Negate },
    { "replica", 'r', 0, Clp_ValString, 0 }
};

int main(int argc, char** argv) {
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options)/sizeof(options[0]), options);
    unsigned n = 0;
    unsigned seed = std::mt19937::default_seed;
    double loss_p = 0.1;
    String configfile;
    String replicaname;
    while (Clp_Next(clp) != Clp_Done) {
        if (Clp_IsLong(clp, "seed"))
            seed = clp->val.u;
        else if (Clp_IsLong(clp, "f")) {
            assert(n == 0);
            n = 2 * clp->val.u + 1;
        } else if (Clp_IsLong(clp, "n")) {
            assert(n == 0);
            n = clp->val.u;
        } else if (Clp_IsLong(clp, "loss")) {
            assert(clp->val.d >= 0 && clp->val.d <= 1);
            loss_p = clp->val.d;
        } else if (Clp_IsLong(clp, "quiet")) {
            if (clp->negated)
                logger.set_frequency(0);
            else
                logger.set_frequency(std::max(logger.frequency(), 1000U) * 2);
        } else if (Clp_IsLong(clp, "config"))
            configfile = clp->negated ? String() : String(clp->vstr);
        else if (Clp_IsLong(clp, "replica"))
            replicaname = clp->vstr;
    }

    Json config;
    if (configfile) {
        String fname = configfile == "-" ? "<stdin>" : configfile;
        FILE* f = configfile == "-" ? stdin : fopen(configfile.c_str(), "r");
        assert(f);
        StringAccum sa;
        while (!feof(f))
            sa.extend(fread(sa.reserve(8192), 1, 8192, f));
        fclose(f);
        config = Json::parse(sa.take_string());
        if (!config || !config.is_o() || !config["members"]) {
            std::cerr << fname << ": parse error\n";
            exit(1);
        }
    } else
        tamer::set_time_type(tamer::time_virtual);
    tamer::initialize();

    if (config)
        run_config(config, replicaname);
    else
        run_test(seed, loss_p, n ? n : 5);

    tamer::cleanup();
}
