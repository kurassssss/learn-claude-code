import { Router, type IRouter } from "express";
import healthRouter from "./health";
import { swarmRouter } from "./swarm";

const router: IRouter = Router();

router.use(healthRouter);
router.use(swarmRouter);

export default router;
