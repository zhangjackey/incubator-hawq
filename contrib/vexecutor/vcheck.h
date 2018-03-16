#ifndef VCHECK_H
#define VCHECK_H

extern Plan* CheckPlanVectorzied(PlannerInfo *root, Plan *plan);
extern Plan* ReplacePlanVectorzied(PlannerInfo *root, Plan *plan);
extern Oid GetVtype(Oid ntype);
extern Oid GetNtype(Oid vtype);

#endif
