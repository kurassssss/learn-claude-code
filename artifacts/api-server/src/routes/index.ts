import { Router, type IRouter } from "express";
import healthRouter from "./health";
import { swarmRouter } from "./swarm";
import { modeRouter } from "./mode";

const router: IRouter = Router();

router.use(healthRouter);
router.use(swarmRouter);
router.use(modeRouter);

export default router;
