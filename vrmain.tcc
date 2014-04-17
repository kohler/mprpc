// -*- mode: c++ -*-
#include "vrtest.hh"
#include "vrnetchannel.hh"
#include "vrreplica.hh"
#include "vrclient.hh"
#include "vrstate.hh"
#include "fsstate.hh"
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
    twait { client->connect(make_event()); }
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
void run_fsreplica(const Vrview& config, String replicaname) {
    std::mt19937 rg(replicaname.hashcode() + time(0));
    vrconstants.trim_log = false;

    auto my_mem = config.find_pointer(replicaname);
    assert(my_mem && my_mem->peer_name["port"].is_u());
    Vrnetlistener* my_conn = new Vrnetlistener(replicaname, my_mem->peer_name["port"].to_u(), rg);
    assert(my_conn->ok());
    Vrreplica* me = new Vrreplica(new Fsstate, config, my_conn, rg);
    me->join(config, event<>());

    tamer::loop();
}

tamed void run_fsclientreq(Vrclient* client, Json clientreq) {
    tamed { Json response; }
    twait { client->connect(make_event()); }
    twait { client->request(std::move(clientreq), make_event(response)); }
    std::cout << response << "\n";
    delete client;
}

void run_fsclient(const Vrview& config, Json clientreq) {
    std::mt19937 rg(time(0));
    Vrnetlistener* conn = new Vrnetlistener("c." + Vrchannel::random_uid(rg),
                                            0, rg);
    Vrclient* client = new Vrclient(conn, config, rg);
    run_fsclientreq(client, std::move(clientreq));
    tamer::loop();
}

tamed void start_run_killreplicas(const Vrview& config,
                                  std::vector<String> uids) {
    tamed {
        std::mt19937 rg(time(0));
        Vrnetlistener conn("k." + Vrchannel::random_uid(rg), 0, rg);
        Vrchannel* chan;
        const Vrview::member_type* mem;
        size_t i;
        std::set<String> killed;
    }
    for (i = 0; i != uids.size(); ++i)
        if (killed.count(uids[i]))
            /* skip */;
        else if ((mem = config.find_pointer(uids[i]))) {
            killed.insert(mem->uid);
            twait { conn.connect(mem->uid, mem->peer_name, make_event(chan)); }
            if (chan) {
                twait { chan->send(Json::array("kill"), make_event()); }
                std::cerr << mem->uid << ": killed\n";
                delete chan;
            } else
                std::cerr << mem->uid << ": connection failed\n";
        } else if (uids[i] == "all") {
            for (auto it = config.members.begin(); it != config.members.end(); ++it)
                uids.push_back(it->uid);
        } else
            std::cerr << uids[i] << ": not a member of the configuration\n";
}

void run_killreplicas(const Vrview& config, std::vector<String> uids) {
    start_run_killreplicas(config, std::move(uids));
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
    { "replica", 'r', 0, Clp_ValString, 0 },
    { "kill", 'k', 0, Clp_ValString, 0 }
};

int main(int argc, char** argv) {
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options)/sizeof(options[0]), options);
    unsigned n = 0;
    unsigned seed = std::mt19937::default_seed;
    double loss_p = 0.1;
    String configfile;
    String replicaname;
    Json clientreq;
    std::vector<String> killreplicas;

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
        else if (Clp_IsLong(clp, "kill"))
            killreplicas.push_back(clp->vstr);
        else if (clp->option->option_id == Clp_NotOption) {
            if (!clientreq)
                clientreq = Json::array();
            Json j = Json::parse(clp->vstr);
            clientreq.push_back(j.is_null() ? Json(clp->vstr) : std::move(j));
        }
    }

    Vrview config;
    if (configfile) {
        String fname = configfile == "-" ? "<stdin>" : configfile;
        FILE* f = configfile == "-" ? stdin : fopen(configfile.c_str(), "r");
        assert(f);
        StringAccum sa;
        while (!feof(f))
            sa.extend(fread(sa.reserve(8192), 1, 8192, f));
        fclose(f);
        Json configj = Json::parse(sa.take_string());
        if (!configj || !config.assign_parse(configj, false, String())) {
            std::cerr << fname << ": parse error\n";
            exit(1);
        }
    } else
        tamer::set_time_type(tamer::time_virtual);
    tamer::initialize();

    assert(!config.empty()
           || (!replicaname && !clientreq && killreplicas.empty()));
    assert(!(replicaname && clientreq));

    if (!config.empty() && !killreplicas.empty())
        run_killreplicas(config, std::move(killreplicas));
    if (!config.empty() && replicaname)
        run_fsreplica(config, replicaname);
    else if (!config.empty() && clientreq)
        run_fsclient(config, clientreq);
    else if (config.empty())
        run_test(seed, loss_p, n ? n : 5);

    tamer::cleanup();
}
