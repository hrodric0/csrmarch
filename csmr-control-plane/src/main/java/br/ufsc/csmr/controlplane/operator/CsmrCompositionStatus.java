package br.ufsc.csmr.controlplane.operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Status subresource for the CsmrComposition CRD.
 *
 * Possible phases: Pending | Ready | Invalid | Warning
 *
 * The "Warning" phase indicates that the composition is valid but has
 * non-blocking warnings (e.g., load balancing issues) that should be addressed.
 */
public class CsmrCompositionStatus {

    private String phase = "Pending";
    private String message = "";

    /** List of blocking violations */
    private List<String> violations = new ArrayList<>();

    /** List of non-blocking warnings */
    private List<String> warnings = new ArrayList<>();

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public List<String> getViolations() { return violations; }
    public void setViolations(List<String> violations) { this.violations = violations; }

    public List<String> getWarnings() { return warnings; }
    public void setWarnings(List<String> warnings) { this.warnings = warnings; }
}