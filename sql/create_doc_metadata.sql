CREATE COLUMN TABLE "DIPROJECTS"."DOC_METADATA"(
	"TEXT_ID" BIGINT  NOT NULL,
	"DATE" DATE  NOT NULL,
	"MEDIA" NVARCHAR(50),
	"LANGUAGE" NVARCHAR(2),
	"URL" NVARCHAR(255),
	"RUBRIC" NVARCHAR(50),
	"TITLE" NVARCHAR(255),
	"NUM_CHARACTERS" BIGINT,
	"PAYWALL" BOOLEAN,
	PRIMARY KEY (
		"TEXT_ID",
		"DATE"
	)
)
