// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::authority::authority_notify_read::Registration;
use crate::consensus_adapter::consensus_tests::test_gas_objects;
use crate::quorum_driver::AuthorityAggregator;
use crate::{
    authority::authority_notify_read::NotifyRead,
    quorum_driver::{QuorumDriverHandler, QuorumDriverMetrics},
    test_utils::init_local_authorities,
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};
use sui_types::crypto::AccountKeyPair;
use sui_types::messages::VerifiedTransaction;
use sui_types::quorum_driver_types::{QuorumDriverError, QuorumDriverResult};
use sui_types::{base_types::TransactionDigest, messages::QuorumDriverResponse};
use crate::authority_client::LocalAuthorityClient;
use crate::test_utils::{make_transfer_sui_transaction, test_account_keys};
use sui_types::base_types::SuiAddress;
use sui_types::crypto::get_key_pair;
use sui_types::object::Object;

async fn setup() -> (
    AuthorityAggregator<LocalAuthorityClient>,
    VerifiedTransaction,
) {
    let (sender, keypair): (_, AccountKeyPair) = get_key_pair();
    let gas_object = Object::with_owner_for_testing(sender);
    let (aggregator, _, _) = init_local_authorities(4, vec![gas_object.clone()]).await;

    let tx = make_tx(&gas_object, sender, &keypair);
    (aggregator, tx)
}

fn make_tx(gas: &Object, sender: SuiAddress, keypair: &AccountKeyPair) -> VerifiedTransaction {
    make_transfer_sui_transaction(
        gas.compute_object_reference(),
        SuiAddress::random_for_testing_only(),
        None,
        sender,
        keypair,
    )
}

#[tokio::test]
async fn test_quorum_driver_submit_transaction() {
    let (aggregator, tx) = setup().await;
    let digest = *tx.digest();
    let quorum_driver_handler = Arc::new(QuorumDriverHandler::new(
        Arc::new(aggregator),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    ));

    // Test submit_transaction
    let qd_clone = quorum_driver_handler.clone();
    let handle = tokio::task::spawn(async move {
        let QuorumDriverResponse {
            tx_cert,
            effects_cert,
        } = qd_clone.subscribe_to_effects().recv().await.unwrap();
        assert_eq!(*tx_cert.digest(), digest);
        assert_eq!(effects_cert.data().transaction_digest, digest);
    });
    let ticket = quorum_driver_handler.submit_transaction(tx).await.unwrap();
    verify_ticket_response(ticket, &digest).await;

    handle.await.unwrap();
}

#[tokio::test]
async fn test_quorum_driver_submit_transaction_no_ticket() {
    let (aggregator, tx) = setup().await;
    let digest = *tx.digest();
    let quorum_driver_handler = Arc::new(QuorumDriverHandler::new(
        Arc::new(aggregator),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    ));
    let qd_clone = quorum_driver_handler.clone();
    let handle = tokio::task::spawn(async move {
        let QuorumDriverResponse {
            tx_cert,
            effects_cert,
        } = qd_clone.subscribe_to_effects().recv().await.unwrap();
        assert_eq!(*tx_cert.digest(), digest);
        assert_eq!(effects_cert.data().transaction_digest, digest);
    });
    quorum_driver_handler
        .submit_transaction_no_ticket(tx)
        .await
        .unwrap();
    handle.await.unwrap();
}

async fn verify_ticket_response<'a>(
    ticket: Registration<'a, TransactionDigest, QuorumDriverResult>,
    tx_digest: &TransactionDigest,
) {
    let QuorumDriverResponse {
        tx_cert,
        effects_cert,
    } = ticket.await.unwrap();
    assert_eq!(tx_cert.digest(), tx_digest);
    assert_eq!(&effects_cert.data().transaction_digest, tx_digest);
}

#[tokio::test]
async fn test_quorum_driver_with_given_notify_read() {
    let (aggregator, tx) = setup().await;
    let digest = *tx.digest();
    let notifier = Arc::new(NotifyRead::default());
    let quorum_driver_handler = Arc::new(QuorumDriverHandler::new_with_notify_read(
        Arc::new(aggregator),
        notifier.clone(),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    ));
    let qd_clone = quorum_driver_handler.clone();
    let handle = tokio::task::spawn(async move {
        let QuorumDriverResponse {
            tx_cert,
            effects_cert,
        } = qd_clone.subscribe_to_effects().recv().await.unwrap();
        assert_eq!(*tx_cert.digest(), digest);
        assert_eq!(effects_cert.data().transaction_digest, digest);
    });
    let ticket1 = notifier.register_one(&digest);
    let ticket2 = quorum_driver_handler.submit_transaction(tx).await.unwrap();
    verify_ticket_response(ticket1, &digest).await;
    verify_ticket_response(ticket2, &digest).await;

    handle.await.unwrap();
}

// TODO: add other cases for mismatched validator/client epoch
#[tokio::test]
async fn test_quorum_driver_update_validators() {
    let (mut aggregator, tx) = setup().await;
    let arc_aggregator = Arc::new(aggregator.clone());
    let quorum_driver_handler = QuorumDriverHandler::new(
        arc_aggregator.clone(),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    );
    let quorum_driver = quorum_driver_handler.clone_quorum_driver();
    let quorum_driver_clone = quorum_driver.clone();
    let handle = tokio::task::spawn(async move {
        // Wait till the epoch/committee is updated.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // This now will fail due to server/client epoch mismatch:
        // server's epoch is 0 but client's is 10
        // This error should not happen in practice for benign validators and a working client
        match tokio::time::timeout(Duration::from_secs(3), quorum_driver.submit_transaction(tx).await.unwrap()).await {
            Err(_) => (),
            _ => panic!("The transaction should err on SafeClient epoch check mismatch, be retried and time out"),
        };
    });

    // Update authority aggregator with a new epoch number, and let quorum driver know.
    aggregator.committee.epoch = 10;
    quorum_driver_clone
        .update_validators(Arc::new(aggregator))
        .await
        .unwrap();
    assert_eq!(
        quorum_driver_handler.clone_quorum_driver().current_epoch(),
        10
    );

    handle.await.unwrap();
}

#[tokio::test]
async fn test_quorum_driver_retry_on_object_locked() -> Result<(), anyhow::Error> {
    let mut gas_objects = test_gas_objects();
    let (sender, keypair) = test_account_keys().pop().unwrap();

    let (aggregator, _, _) = init_local_authorities(4, gas_objects.clone()).await;
    let aggregator = Arc::new(aggregator);
    let quorum_driver_handler = QuorumDriverHandler::new(
        aggregator.clone(),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    );
    let quorum_driver = quorum_driver_handler.clone_quorum_driver();

    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    let names: Vec<_> = aggregator.authority_clients.keys().clone().collect();
    assert_eq!(names.len(), 4);
    let client0 = aggregator.clone_client(names[0]);
    let client1 = aggregator.clone_client(names[1]);
    let client2 = aggregator.clone_client(names[2]);

    // Case 1 - two validators lock the object with the same tx
    assert!(client0.handle_transaction(tx.clone()).await.is_ok());
    assert!(client1.handle_transaction(tx.clone()).await.is_ok());

    let tx2 = make_tx(&gas, sender, &keypair);
    let res = quorum_driver.submit_transaction(tx2).await.unwrap().await;
    // If aggregator gets two bad responses from 0 and 1 before getting two good responses from 2 and 3,
    // it will retry tx, but it will fail due to equivocaiton.
    // If aggregator gets two good responses from client 2 and 3 before two bad responses from 0 and 1,
    // tx will not be retried.
    if let Err(QuorumDriverError::ObjectsDoubleUsed {
        conflicting_txes, ..
    }) = res
    {
        assert_eq!(conflicting_txes.len(), 1);
        assert_eq!(conflicting_txes.iter().next().unwrap().0, tx.digest());
    } else {
        panic!(
            "expect Err(QuorumDriverError::ObjectsDoubleUsed) but got {:?}",
            res
        );
    }

    // Case 2 - three validators lock the object with the same tx
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);

    assert!(client0.handle_transaction(tx.clone()).await.is_ok());
    assert!(client1.handle_transaction(tx.clone()).await.is_ok());
    assert!(client2.handle_transaction(tx.clone()).await.is_ok());

    let tx2 = make_tx(&gas, sender, &keypair);

    let res = quorum_driver.submit_transaction(tx2).await.unwrap().await;
    // Aggregator gets three bad responses, and tries tx, which should succeed.
    if let Err(QuorumDriverError::ObjectsDoubleUsed {
        conflicting_txes,
        retried_tx,
        retried_tx_success,
    }) = res
    {
        assert_eq!(retried_tx, Some(*tx.digest()));
        assert_eq!(retried_tx_success, Some(true));
        assert_eq!(conflicting_txes.len(), 1);
        assert_eq!(conflicting_txes.iter().next().unwrap().0, tx.digest());
    } else {
        panic!("expect Err(QuorumDriverError::ObjectsDoubleUsed) but got {:?}", res)
    }

    // Case 3 - one validator locks the object
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    assert!(client0.handle_transaction(tx.clone()).await.is_ok());

    let tx2 = make_tx(&gas, sender, &keypair);
    let tx2_digest = *tx2.digest();

    let res = quorum_driver
        .submit_transaction(tx2)
        .await
        .unwrap()
        .await
        .unwrap();

    // Aggregator gets three good responses and execution succeeds.
    let QuorumDriverResponse {
        tx_cert,
        effects_cert: _,
    } = res;
    assert_eq!(tx_cert.digest(), &tx2_digest);

    // Case 4 - object is locked by 2 txes with weight 2 and 1 respectivefully. Then try to execute the third txn
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    let tx2 = make_tx(&gas, sender, &keypair);

    assert!(client0.handle_transaction(tx.clone()).await.is_ok());
    assert!(client1.handle_transaction(tx.clone()).await.is_ok());
    assert!(client2.handle_transaction(tx2.clone()).await.is_ok());

    let tx3 = make_tx(&gas, sender, &keypair);

    let res = quorum_driver.submit_transaction(tx3).await.unwrap().await;

    // If aggregator gets two bad responses from 0 and 1, it will retry tx, but it will fail due to equivocaiton.
    // If aggregator gets two bad responses of which one from 2, then no tx will be retried.
    if !matches!(res, Err(QuorumDriverError::ObjectsDoubleUsed { .. })) {
        panic!("expect Err(QuorumDriverError::ObjectsDoubleUsed) but got {:?}", res)
    }

    // Case 5 - object is locked by 2 txes with weight 2 and 1, try to execute the lighter stake tx
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    let tx2 = make_tx(&gas, sender, &keypair);
    assert!(client0.handle_transaction(tx.clone()).await.is_ok());
    assert!(client1.handle_transaction(tx.clone()).await.is_ok());
    assert!(client2.handle_transaction(tx2.clone()).await.is_ok());
    println!("tx2: {:?}", tx2.digest());
    let res = quorum_driver.submit_transaction(tx2).await.unwrap().await;

    // if aggregator gets two bad responses from 0 and 1 first, it will try to retry tx (stake = 2), but that will fail
    // If aggregator gets two bad responses of which one from 2, then no tx will be retried.
    if !matches!(res, Err(QuorumDriverError::ObjectsDoubleUsed { .. })) {
        panic!("expect Err(QuorumDriverError::ObjectsDoubleUsed) but got {:?}", res)
    }

    // Case 6 - object is locked by 2 txes with weight 2 and 1, try to execute the heavier stake tx
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    let tx_digest = *tx.digest();
    let tx2 = make_tx(&gas, sender, &keypair);

    assert!(client0.handle_transaction(tx.clone()).await.is_ok());
    assert!(client1.handle_transaction(tx.clone()).await.is_ok());
    assert!(client2.handle_transaction(tx2).await.is_ok());

    let res = quorum_driver
        .submit_transaction(tx)
        .await
        .unwrap()
        .await
        .unwrap();

    let QuorumDriverResponse {
        tx_cert,
        effects_cert: _,
    } = res;
    assert_eq!(*tx_cert.digest(), tx_digest);

    // Case 7 - three validators lock the object, by different txes
    let gas = gas_objects.pop().unwrap();
    let tx = make_tx(&gas, sender, &keypair);
    let tx2 = make_tx(&gas, sender, &keypair);
    let tx3 = make_tx(&gas, sender, &keypair);
    assert!(client0.handle_transaction(tx).await.is_ok());
    assert!(client1.handle_transaction(tx2).await.is_ok());
    assert!(client2.handle_transaction(tx3).await.is_ok());

    let tx4 = make_tx(&gas, sender, &keypair);
    let res = quorum_driver.submit_transaction(tx4).await.unwrap().await;

    if !matches!(res, Err(QuorumDriverError::ObjectsDoubleUsed { .. })) {
        panic!(
            "expect Err(QuorumDriverError::ObjectsEquivocated) but got {:?}",
            res
        )
    }

    Ok(())
}

#[tokio::test]
async fn test_quorum_driver_not_retry_on_object_locked() -> Result<(), anyhow::Error> {
    let (auth_agg, _) = setup().await;
    let quorum_driver_handler = QuorumDriverHandler::new_with_notify_read(
        Arc::new(auth_agg.clone()),
        Arc::new(NotifyRead::new()),
        Arc::new(QuorumDriverMetrics::new_for_tests()),
    );
    let quorum_driver = quorum_driver_handler.clone_quorum_driver();
    let validity = quorum_driver
        .authority_aggregator()
        .load()
        .committee
        .validity_threshold();

    assert_eq!(auth_agg.clone_inner_clients().keys().cloned().count(), 4);

    // good stake >= validity, no transaction will be retried, expect Ok(None)
    assert_eq!(
        quorum_driver
            .attempt_conflicting_transactions_maybe(
                validity,
                &BTreeMap::new(),
                &TransactionDigest::random()
            )
            .await,
        Ok(None)
    );
    assert_eq!(
        quorum_driver
            .attempt_conflicting_transactions_maybe(
                validity + 1,
                &BTreeMap::new(),
                &TransactionDigest::random()
            )
            .await,
        Ok(None)
    );

    // good stake < validity, but the top transaction total stake < validaty too, no transaction will be retried, expect Ok(None)
    let conflicting_tx_digests = BTreeMap::from([
        (TransactionDigest::random(), (vec![], validity - 1)),
        (TransactionDigest::random(), (vec![], 1)),
    ]);
    assert_eq!(
        quorum_driver
            .attempt_conflicting_transactions_maybe(
                validity - 1,
                &conflicting_tx_digests,
                &TransactionDigest::random()
            )
            .await,
        Ok(None)
    );

    Ok(())
}
