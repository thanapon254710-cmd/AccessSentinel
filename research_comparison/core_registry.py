"""Registry for the six selected related-paper core implementations.

The registry records core classes separately from the common-data adapters. A working
adapter is a documented domain adaptation, not a claim of exact paper reproduction.
"""
from .methods.ref24_user_resource_embedding import UserResourceSpectralNN, Ref24Config
from .methods.ref14_dynad import DynADCore, GraphSnapshot
from .methods.ref17_pg_aid import PGAIDCore, TemporalSnapshotBuilder, SnapshotTransformer
from .methods.ref21_acobe import ACOBECore, AspectAutoencoder, CompoundDeviationBuilder
from .methods.ref22_fgdt import FGDTCore, CandidateGroup, HINRNNSpatial, TemporalCollaborationRNN
from .methods.ref25_deepro import DEEPROCore, MAGNNProcessDetector, HeterogeneousProvenanceGraph

SELECTED_REFERENCES = {
    24: {"name":"User-resource graph embedding","core":UserResourceSpectralNN},
    14: {"name":"DynAD temporal graph network","core":DynADCore},
    17: {"name":"PG-AID provenance graph detector","core":PGAIDCore},
    21: {"name":"ACOBE group-behavior detector","core":ACOBECore},
    22: {"name":"FGDT spatio-temporal group detector","core":FGDTCore},
    25: {"name":"DEEPRO provenance GNN","core":DEEPROCore},
}

def implementation_status():
    return {ref: {"method":item["name"],"core_importable":callable(item["core"]),
                  "benchmark_adapter":"implemented",
                  "quantitative_comparison":"quick_smoke_tested_full_run_pending"}
            for ref,item in SELECTED_REFERENCES.items()}
