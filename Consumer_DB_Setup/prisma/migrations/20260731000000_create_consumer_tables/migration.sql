-- CreateTable
CREATE TABLE "Events_consumed" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "topic" TEXT NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" TEXT NOT NULL,
    "order_event" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Events_consumed_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Processed_Event" (
    "order_id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Processed_Event_pkey" PRIMARY KEY ("order_id")
);
