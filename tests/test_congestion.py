"""A4: fixed-time CUBIC vectors and real transport integration."""
import asyncio
import math

import pytest

from common import constants as C
from transport.congestion import CongestionController, CubicController, make_controller
from transport.endpoint import Endpoint
from tests.test_recovery import Clock, Sender
from tests.congestion_scenario import run_scenario


def cubic():
    clock = Clock()
    return clock, CubicController(clock=clock)


def avoidance():
    """A known curve: W_max=23.2, cwnd_epoch=20 gives K=2 seconds."""
    clock, cc = cubic()
    cc.cwnd = cc.ssthresh = 20.0
    cc.w_max = 23.2
    cc.cwnd_prior = 30.0
    cc._restart = cc._first_epoch = False
    cc.on_ack(1, srtt=0.1)
    return clock, cc


def test_cubic_is_default_and_aimd_remains_selectable():
    assert make_controller().algorithm == "cubic"
    assert make_controller("aimd").algorithm == "aimd"
    assert Endpoint().congestion_algorithm == "cubic"
    assert Sender().conn.get_stats().algorithm == "cubic"
    assert Sender("aimd").conn.get_stats().algorithm == "aimd"
    with pytest.raises(ValueError, match="congestion_algorithm"):
        Endpoint(congestion_algorithm="typo")


def test_slow_start_grows_by_new_ack_and_stops_at_threshold():
    clock, cc = cubic()
    assert (cc.cwnd, cc.ssthresh) == (10, 64)
    cc.on_ack(10, srtt=0.1)
    assert cc.cwnd == 20
    cc.ssthresh = 23
    cc.on_ack(3, srtt=0.1)
    assert cc.cwnd == 23 and cc.epoch_start is None
    cc.on_ack(1, srtt=0.1)
    assert cc.epoch_start == clock()
    assert cc.cwnd_epoch == cc.cwnd_prior == cc.w_max == 23
    assert cc.k == 0 and cc.cwnd > 23


def test_known_curve_reno_then_concave_growth_uses_time_and_rtt():
    clock, cc = avoidance()
    assert cc.k == pytest.approx(2)
    assert cc.cwnd == pytest.approx(20.026470588235295)
    assert cc.alpha == pytest.approx(9 / 17)
    clock.advance(1)
    cc.on_ack(1, srtt=0.1)
    # At t=1, cubic_now=22.8 and target(t+0.1)=22.9084.
    assert cc.cwnd == pytest.approx(20.17037659499106)


def test_rtt_changes_cubic_target_with_same_ack_and_elapsed_time():
    clock_a, a = avoidance()
    clock_b, b = avoidance()
    clock_a.advance(1)
    clock_b.advance(1)
    a.on_ack(1, srtt=0.01)
    b.on_ack(1, srtt=0.2)
    assert b.cwnd > a.cwnd


def test_plateau_and_convex_region_do_not_jump_directly_to_target():
    clock, cc = avoidance()
    clock.advance(2)
    cc.cwnd = cc.w_max
    cc.on_ack(1, srtt=0.1)
    # W_cubic(K)=23.2; next-RTT target=23.2004.
    assert cc.cwnd == pytest.approx(23.20001724137931)
    before = cc.cwnd
    clock.advance(1)
    cc.on_ack(1, srtt=0.1)
    assert before < cc.cwnd < 23.7324


def test_cubic_target_is_bounded_even_after_large_active_time():
    clock, cc = avoidance()
    clock.advance(1000)
    before = cc.cwnd
    cc.on_ack(1, srtt=0.1)
    assert cc.cwnd == pytest.approx(before + 0.5)


def test_reno_alpha_becomes_one_after_reaching_prior_window():
    _, cc = avoidance()
    cc.cwnd_prior = cc.w_est
    cc.on_ack(1, srtt=0.1)
    assert cc.alpha == 1


def test_loss_uses_pre_ack_flight_and_remembers_previous_window():
    _, cc = cubic()
    cc.cwnd = 40
    cc.on_loss(flight_size=30)
    assert cc.cwnd == cc.ssthresh == 21
    assert cc.w_max == cc.cwnd_prior == 40
    cc.on_ack(1, srtt=0.1)
    assert cc.k == pytest.approx(math.cbrt(47.5))


@pytest.mark.parametrize("enabled,expected", [(True, 25.5), (False, 30)])
def test_fast_convergence_is_configurable(enabled, expected):
    cc = CubicController(fast_convergence=enabled)
    cc.cwnd = 40
    cc.on_loss(flight_size=40)
    cc.cwnd = 30
    cc.on_loss(flight_size=30)
    assert cc.w_max == pytest.approx(expected)
    assert cc.cwnd == cc.ssthresh == 21


def test_signed_k_keeps_curve_continuous_after_small_flight_loss():
    _, cc = cubic()
    cc.cwnd = 2
    cc.w_max = 40
    cc.on_loss(flight_size=1)
    cc.on_ack(1, srtt=0.1)
    assert cc.k < 0  # Fast convergence w_max=1.7, minimum cwnd=2.
    assert cc.CUBIC_C * (-cc.k) ** 3 + cc.w_max == pytest.approx(cc.cwnd_epoch)


def test_timeout_restarts_slow_start_then_curve_with_k_zero():
    _, cc = cubic()
    cc.cwnd = 40
    cc.on_timeout(flight_size=40)
    assert cc.cwnd == 1 and cc.ssthresh == 28
    cc.on_ack(27, srtt=0.1)
    assert cc.cwnd == 28 and cc.epoch_start is None
    cc.on_ack(1, srtt=0.1)
    assert cc.k == 0 and cc.w_max == 28
    assert cc.w_est == pytest.approx(28 + cc.ALPHA / 28)
    assert cc.cwnd_epoch == 28


def test_timeout_escalation_never_increases_reduced_window():
    _, cc = cubic()
    cc.cwnd = 40
    cc.on_loss(flight_size=40)
    cc.on_timeout(flight_size=40)
    assert cc.cwnd == 1 and cc.ssthresh <= 28


def test_idle_time_is_excluded_and_sparse_acks_do_not_grow_window():
    clock, cc = avoidance()
    clock.advance(1)
    cc.set_application_limited(True)
    before, epoch = cc.cwnd, cc.epoch_start
    clock.advance(100)
    cc.on_ack(5, cwnd_limited=False)
    assert cc.cwnd == before
    cc.set_application_limited(False)
    assert cc.epoch_start == pytest.approx(epoch + 100)
    cc.on_ack(1, srtt=0.1)
    assert cc.cwnd == pytest.approx(20.17037659499106)


def test_zero_or_duplicate_ack_does_not_start_or_resume_epoch():
    _, cc = cubic()
    before = dict(cc.__dict__)
    cc.on_ack(0)
    assert cc.__dict__ == before


def test_batched_ack_is_equivalent_to_individual_new_acks():
    clock_a, a = avoidance()
    clock_b, b = avoidance()
    clock_a.advance(1)
    clock_b.advance(1)
    a.on_ack(5, srtt=0.1)
    for _ in range(5):
        b.on_ack(1, srtt=0.1)
    assert a.cwnd == b.cwnd and a.w_est == b.w_est


def test_window_stays_finite_and_bounded_during_ack_loss_timeout_cycles():
    clock, cc = cubic()
    for i in range(250):
        clock.advance(0.1)
        cc.on_ack(64, srtt=0.1)
        assert math.isfinite(cc.cwnd) and 1 <= cc.cwnd <= 64
        assert 1 <= cc.allowed_inflight() <= 64
        if i % 7 == 0:
            cc.on_timeout(flight_size=cc.allowed_inflight())
        elif i % 5 == 0:
            cc.on_loss(flight_size=cc.allowed_inflight())


def test_original_aimd_baseline_numbers_are_preserved():
    cc = CongestionController()
    cc.on_ack(10)
    assert cc.cwnd == 20
    cc.on_loss(flight_size=1)
    assert cc.cwnd == cc.ssthresh == 10
    cc.on_ack(1)
    assert cc.cwnd == 10.1
    cc.on_timeout()
    assert cc.cwnd == 1 and cc.ssthresh == 5.05


def test_flight_that_filled_window_can_grow_as_acks_drain_it():
    sender = Sender()
    sender.send(10)
    sender.clock.advance(0.1)
    sender.ack(1)
    assert sender.conn.cc.cwnd == 11
    sender.ack(10)
    assert sender.conn.cc.cwnd == 20
    assert sender.conn.cc._limited_since == sender.clock()
    sender.send(1)
    sender.clock.advance(0.1)
    sender.ack(11)
    assert sender.conn.cc.cwnd == 20


def test_credit_limited_sender_does_not_inflate_cwnd_while_waiting():
    sender = Sender()
    stream = sender.conn._send_stream(2)
    stream.peer_credit = (100, 4)
    sender.send(12)
    assert len(sender.data_packets()) == 4
    sender.clock.advance(0.1)
    sender.ack(4)
    assert sender.conn.cc.cwnd == 10
    assert len(stream.pending) == 8 and not stream.unacked
    for _ in range(4):
        sender.tick(1)
    assert sender.conn.cc.cwnd == 10
    assert len(sender.data_packets()) == 4


def test_loss_reduction_captures_flight_before_ack_removes_packets():
    sender = Sender()
    sender.send(10)
    sender.clock.advance(0.1)
    sender.ack(1, 0b111)
    # Four packets just left unacked, but the loss signal observed a flight of 10.
    assert sender.conn.cc.cwnd == 7
    before = sender.conn.cc.cwnd
    sender.ack(10)
    assert sender.conn.cc.cwnd == before


def test_new_data_respects_cwnd_across_multiple_streams():
    sender = Sender()
    sender.send(30, sid=2)
    sender.send(30, sid=3)
    assert len(sender.data_packets()) == sender.conn.cc.allowed_inflight() == 10
    sender.clock.advance(0.1)
    sender.ack(10)
    assert sender.conn._inflight_total() == 20
    assert {pkt.stream_id for pkt in sender.data_packets()[10:]} == {2, 3}


@pytest.mark.parametrize("algorithm", ["cubic", "aimd"])
def test_real_endpoint_passes_selection_and_creates_independent_controllers(algorithm):
    async def run():
        server = Endpoint(True, congestion_algorithm=algorithm)
        clients = [Endpoint(congestion_algorithm=algorithm) for _ in range(2)]
        try:
            await server.start(("127.0.0.1", 0))
            for client in clients:
                await client.start(("127.0.0.1", 0))
                cid = await client.connect(server.local_addr)
                assert client.get_stats(cid).algorithm == algorithm
            first, second = list(server.connections.values())
            assert first.cc is not second.cc
            first.cc.on_timeout()
            assert second.cc.cwnd == C.INITIAL_CWND
        finally:
            for client in clients:
                await client.stop()
            await server.stop()
    asyncio.run(run())


@pytest.mark.parametrize("algorithm", ["aimd", "cubic"])
@pytest.mark.parametrize("scenario", ["clean", "targeted", "loss10"])
def test_fixed_network_scenarios_deliver_exactly_once_and_observe_recovery(algorithm, scenario):
    result, trace = run_scenario(algorithm, scenario)
    assert result["stats"]["acked_payload_bytes"] == result["payload_bytes"]
    assert all(1 <= row["cwnd_packets"] <= C.MAX_WINDOW for row in trace)
    if scenario == "clean":
        assert result["stats"]["retransmitted"] == 0
    elif scenario == "targeted":
        assert result["stats"]["loss_reductions"] == 1
        assert result["stats"]["fast_retransmitted"] == 1
    else:
        assert result["dropped"]["client"] > 0 and result["dropped"]["server"] > 0
        assert result["stats"]["retransmitted"] > 0
