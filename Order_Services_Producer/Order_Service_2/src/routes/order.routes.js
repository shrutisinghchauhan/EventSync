import {Router} from "express";
import {addOrders_Transactional_Outbox_Pattern,addOrders_Transactional_Log_Tailing_Pattern,addOrders_Listen_To_Yourself,getOrders,getOutbox,getOutbox_Listen_To_Yourself, deleteOrders, deleteOutbox, deleteOutbox_Listen_To_Yourself} from "../controllers/order.controller.js";

const router = Router();

router.route("/add-order-top").post(addOrders_Transactional_Outbox_Pattern);
router.route("/add-order-ltu").post(addOrders_Listen_To_Yourself);
router.route("/add-order-tlt").post(addOrders_Transactional_Log_Tailing_Pattern);
router.route("/get-orders").get(getOrders);
router.route("/get-outbox").get(getOutbox);
router.route("/get-outbox-ltu").get(getOutbox_Listen_To_Yourself);
router.route("/delete-orders").delete(deleteOrders);
router.route("/delete-outbox1").delete(deleteOutbox);
router.route("/delete-outbox2").delete(deleteOutbox_Listen_To_Yourself);

export default router;