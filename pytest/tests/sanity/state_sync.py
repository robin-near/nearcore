#!/usr/bin/env python3
# Spins up a node, then waits for couple epochs
# and spins up another node
# Makes sure that eventually the second node catches up
# Three modes:
#   - notx: no transactions are sent, just checks that
#     the second node starts and catches up
#   - onetx: sends one series of txs at the beginning,
#     makes sure the second node balances reflect them
#   - manytx: constantly issues txs throughout the test
#     makes sure the balances are correct at the end

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

if len(sys.argv) < 3:
    logger.info("python state_sync.py [notx, onetx, manytx] <launch_at_block>")
    exit(1)

mode = sys.argv[1]
assert mode in ['notx', 'onetx', 'manytx']

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
import state_sync_lib
import utils

START_AT_BLOCK = int(sys.argv[2])
TIMEOUT = 150 + START_AT_BLOCK * 10

config = load_config()

node_config = state_sync_lib.get_state_sync_config_combined()
node_config["consensus.block_fetch_horizon"] = 3
node_config["consensus.block_header_fetch_horizon"] = 10

near_root, node_dirs = init_cluster(
    2, 1, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]],
    {x: node_config for x in range(3)})

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)

ctx = utils.TxContext([0, 0], [boot_node, node1])

sent_txs = False

observed_height = 0
for height, block_hash in utils.poll_blocks(boot_node,
                                            timeout=TIMEOUT,
                                            poll_interval=0.1):
    observed_height = height
    if height >= START_AT_BLOCK:
        break

    if mode == 'onetx' and not sent_txs:
        ctx.send_moar_txs(block_hash, 3, False)
        sent_txs = True
    elif mode == 'manytx':
        if ctx.get_balances() == ctx.expected_balances:
            ctx.send_moar_txs(block_hash, 3, False)
            logger.info(f'Sending moar txs at height {height}')

if mode == 'onetx':
    assert ctx.get_balances() == ctx.expected_balances

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
tracker = utils.LogTracker(node2)
time.sleep(3)

utils.wait_for_blocks(node2, target=200)

catch_up_height = 0
for height, block_hash in utils.poll_blocks(boot_node,
                                            timeout=TIMEOUT,
                                            poll_interval=0.1):
    catch_up_height = height
    if height >= observed_height + 100:
        break
    if mode == 'manytx' and ctx.get_balances() == ctx.expected_balances:
        boot_height = boot_node.get_latest_block().height
        ctx.send_moar_txs(block_hash, 3, False)
        logger.info(f'Sending moar txs at height {boot_height}')

boot_heights = boot_node.get_all_heights()

assert catch_up_height in boot_heights, "%s not in %s" % (catch_up_height,
                                                          boot_heights)

tracker.reset(
)  # the transition might have happened before we initialized the tracker
if catch_up_height >= 100:
    assert tracker.check("transition to State Sync")
elif catch_up_height <= 30:
    assert not tracker.check("transition to State Sync")

if mode == 'manytx':
    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the old node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)

    # requery the balances from the newly started node
    ctx.nodes.append(node2)
    ctx.act_to_val = [2, 2, 2]

    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the new node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)

logger.info('EPIC')
