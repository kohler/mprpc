// -*- mode: c++ -*-
#include "vrtest.hh"
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

static Clp_Option options[] = {
    { "f", 'f', 0, Clp_ValUnsigned, 0 },
    { "loss", 'l', 0, Clp_ValDouble, 0 },
    { "n", 'n', 0, Clp_ValUnsigned, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate },
    { "seed", 's', 0, Clp_ValUnsigned, 0 }
};

int main(int argc, char** argv) {
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options)/sizeof(options[0]), options);
    unsigned n = 0;
    unsigned seed = std::mt19937::default_seed;
    double loss_p = 0.1;
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
        }
    }
    n = n ? n : 5;

    tamer::set_time_type(tamer::time_virtual);
    tamer::initialize();

    Vrtestcollection vrg(seed, loss_p);
    std::vector<Vrreplica*> nodes;
    for (unsigned i = 0; i < n; ++i)
        nodes.push_back(vrg.add_replica(make_replica_uid()));

    go(vrg, nodes);

    while (1) {
        tamer::once();
        vrg.check();
    }

    tamer::cleanup();
}
