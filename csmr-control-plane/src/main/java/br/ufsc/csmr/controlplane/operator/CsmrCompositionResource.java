package br.ufsc.csmr.controlplane.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Kubernetes Custom Resource (CRD) for CSMR compositions.
 *
 * apiVersion: csmr.ufsc.br/v1alpha1
 * kind: CsmrComposition
 */
@Group("csmr.ufsc.br")
@Version("v1alpha1")
public class CsmrCompositionResource
        extends CustomResource<CsmrCompositionSpec, CsmrCompositionStatus>
        implements Namespaced {
}
