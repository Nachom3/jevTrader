# Jev Lead-Lag V1 questions

Each `##` section is one System One question. `instructions` is a literal block so the wording remains reviewable and parseable.

## yes_pressure_5s
type: noul
instructions: |
  Does the current external market state in `underlying` imply an increase in the probability of YES over the next 5 seconds?

## no_pressure_5s
type: noul
instructions: |
  Does the current external market state in `underlying` imply a decrease in the probability of YES over the next 5 seconds?

## move_persists
type: noul
instructions: |
  Is the current external move in `underlying` likely to persist over the next seconds rather than immediately mean-revert?

## underreact_up
type: noul
instructions: |
  Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should increase P(YES)?

## underreact_down
type: noul
instructions: |
  Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should decrease P(YES)?

## repricing_ticks
type: choice
instructions: |
  What is the most likely Polymarket YES price movement over the next 5 seconds?
criteria:
  UP_3_PLUS_TICKS: YES rises by 3 or more minimum price increments
  UP_2_TICKS: YES rises by 2 minimum price increments
  UP_1_TICK: YES rises by 1 minimum price increment
  FLAT: YES stays within the current tick
  DOWN_1_TICK: YES falls by 1 minimum price increment
  DOWN_2_TICKS: YES falls by 2 minimum price increments
  DOWN_3_PLUS_TICKS: YES falls by 3 or more minimum price increments

## fill_before_decay
type: noul
instructions: |
  Is the maker order in `candidate_order` (BUY YES at {candidate_price}) likely to be filled before the current informational advantage disappears?

## fill_toxic
type: noul
instructions: |
  If the maker order in `candidate_order` (BUY YES at {candidate_price}) gets filled, is the fill likely to occur because the market is moving adversely against that quote?
