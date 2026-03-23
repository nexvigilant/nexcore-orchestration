//! Consensus rule definitions.

use serde::{Deserialize, Serialize};

/// Rule for evaluating whether consensus has been achieved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusRule {
    /// At least N agents must agree on the same value.
    MinAgree(usize),
    /// More than half of all agents must agree.
    Majority,
    /// All agents must agree on the same value.
    Unanimous,
    /// At least one agent must produce a result.
    Any,
    /// A fraction (0.0–1.0) of all agents must agree.
    Quorum(f64),
}

impl ConsensusRule {
    /// Check if the rule is satisfied given agreement count and total agents.
    #[must_use]
    pub fn is_satisfied(&self, agreement_count: usize, total: usize) -> bool {
        if total == 0 {
            return false;
        }
        match self {
            Self::MinAgree(n) => agreement_count >= *n,
            Self::Majority => agreement_count > total / 2,
            Self::Unanimous => agreement_count == total,
            Self::Any => agreement_count >= 1,
            Self::Quorum(fraction) => {
                let required = (*fraction * total as f64).ceil() as usize;
                agreement_count >= required
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_agree() {
        assert!(ConsensusRule::MinAgree(2).is_satisfied(2, 5));
        assert!(ConsensusRule::MinAgree(2).is_satisfied(3, 5));
        assert!(!ConsensusRule::MinAgree(2).is_satisfied(1, 5));
    }

    #[test]
    fn majority() {
        assert!(ConsensusRule::Majority.is_satisfied(3, 5));
        assert!(ConsensusRule::Majority.is_satisfied(2, 3));
        assert!(!ConsensusRule::Majority.is_satisfied(2, 5));
        assert!(!ConsensusRule::Majority.is_satisfied(1, 2));
    }

    #[test]
    fn unanimous() {
        assert!(ConsensusRule::Unanimous.is_satisfied(5, 5));
        assert!(!ConsensusRule::Unanimous.is_satisfied(4, 5));
    }

    #[test]
    fn any() {
        assert!(ConsensusRule::Any.is_satisfied(1, 10));
        assert!(!ConsensusRule::Any.is_satisfied(0, 10));
    }

    #[test]
    fn quorum() {
        // 60% of 5 = 3 needed
        assert!(ConsensusRule::Quorum(0.6).is_satisfied(3, 5));
        assert!(!ConsensusRule::Quorum(0.6).is_satisfied(2, 5));
    }

    #[test]
    fn zero_total_never_satisfied() {
        assert!(!ConsensusRule::Any.is_satisfied(0, 0));
        assert!(!ConsensusRule::Majority.is_satisfied(0, 0));
        assert!(!ConsensusRule::Unanimous.is_satisfied(0, 0));
    }
}
