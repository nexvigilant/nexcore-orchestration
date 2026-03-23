//! # GroundsTo implementations for nexcore-orchestration types
//!
//! Connects agent lifecycle, registry, consensus, and work queue types
//! to the Lex Primitiva type system.
//!
//! ## Domain Signature
//!
//! - **ς (State)**: agent lifecycle state machine
//! - **σ (Sequence)**: work queue ordering, lifecycle transitions
//! - **∂ (Boundary)**: concurrency limits, backpressure

use nexcore_lex_primitiva::grounding::GroundsTo;
use nexcore_lex_primitiva::primitiva::{LexPrimitiva, PrimitiveComposition};

use crate::agent::{AgentRecord, AgentState};
use crate::error::OrcError;
use crate::types::{AgentId, Priority, TaskGroupId};

// ---------------------------------------------------------------------------
// T2-P: Identity types
// ---------------------------------------------------------------------------

/// AgentId: T2-P (λ + ∃), dominant λ
///
/// Unique agent identifier backed by UUID. Location-dominant: naming.
impl GroundsTo for AgentId {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Location,  // λ -- unique address
            LexPrimitiva::Existence, // ∃ -- agent exists
        ])
        .with_dominant(LexPrimitiva::Location, 0.90)
    }
}

/// TaskGroupId: T2-P (λ + ∃), dominant λ
///
/// Task group identifier. Location-dominant: group addressing.
impl GroundsTo for TaskGroupId {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Location,  // λ -- group address
            LexPrimitiva::Existence, // ∃ -- group exists
        ])
        .with_dominant(LexPrimitiva::Location, 0.90)
    }
}

// ---------------------------------------------------------------------------
// T2-P: Enums
// ---------------------------------------------------------------------------

/// Priority: T2-P (κ + N), dominant κ
///
/// Task priority levels. Comparison-dominant: ordering is the purpose.
impl GroundsTo for Priority {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Comparison, // κ -- priority ordering
            LexPrimitiva::Quantity,   // N -- numeric weight
        ])
        .with_dominant(LexPrimitiva::Comparison, 0.90)
    }
}

/// AgentState: T2-P (ς + σ), dominant ς
///
/// Agent lifecycle state machine: Queued -> Acquiring -> Executing -> Done.
/// State-dominant: the type IS a state machine position.
impl GroundsTo for AgentState {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,    // ς -- current position in FSM
            LexPrimitiva::Sequence, // σ -- ordered transitions
        ])
        .with_dominant(LexPrimitiva::State, 0.90)
    }
}

// ---------------------------------------------------------------------------
// T3: Domain aggregates
// ---------------------------------------------------------------------------

/// AgentRecord: T3 (ς + λ + σ + κ + N + ∃), dominant ς
///
/// Full lifecycle record of an agent. State-dominant: tracks lifecycle.
impl GroundsTo for AgentRecord {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::State,      // ς -- lifecycle state
            LexPrimitiva::Location,   // λ -- agent identity
            LexPrimitiva::Sequence,   // σ -- temporal ordering
            LexPrimitiva::Comparison, // κ -- priority
            LexPrimitiva::Quantity,   // N -- durations, timestamps
            LexPrimitiva::Existence,  // ∃ -- group membership
        ])
        .with_dominant(LexPrimitiva::State, 0.80)
    }
}

// ---------------------------------------------------------------------------
// Error types — ∂ dominant
// ---------------------------------------------------------------------------

/// OrcError: T2-C (∂ + ∅ + ς + N), dominant ∂
///
/// Orchestration errors: boundary violations (queue full, concurrency limit,
/// timeout), state machine violations, missing agents.
impl GroundsTo for OrcError {
    fn primitive_composition() -> PrimitiveComposition {
        PrimitiveComposition::new(vec![
            LexPrimitiva::Boundary, // ∂ -- capacity limits, timeouts
            LexPrimitiva::Void,     // ∅ -- not found
            LexPrimitiva::State,    // ς -- invalid state transitions
            LexPrimitiva::Quantity, // N -- capacity values
        ])
        .with_dominant(LexPrimitiva::Boundary, 0.85)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use nexcore_lex_primitiva::tier::Tier;

    #[test]
    fn agent_id_is_location_dominant() {
        assert_eq!(AgentId::dominant_primitive(), Some(LexPrimitiva::Location));
        assert_eq!(AgentId::tier(), Tier::T2Primitive);
    }

    #[test]
    fn priority_is_comparison_dominant() {
        assert_eq!(
            Priority::dominant_primitive(),
            Some(LexPrimitiva::Comparison)
        );
    }

    #[test]
    fn agent_state_is_state_dominant() {
        assert_eq!(AgentState::dominant_primitive(), Some(LexPrimitiva::State));
    }

    #[test]
    fn agent_record_is_t3() {
        assert_eq!(AgentRecord::tier(), Tier::T3DomainSpecific);
        assert_eq!(AgentRecord::dominant_primitive(), Some(LexPrimitiva::State));
    }

    #[test]
    fn orc_error_is_boundary_dominant() {
        assert_eq!(OrcError::dominant_primitive(), Some(LexPrimitiva::Boundary));
    }
}
