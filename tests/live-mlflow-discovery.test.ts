// Focused regression pin for the MLflow code-environment discovery rule:
// deploymentMode "DSS_INTERNAL" environments must never be auto-selected, even
// when they ship mlflow — the live iteration-22 incident selected
// INTERNAL_retrieval_augmented_generation_v1 because the guard compared the env
// NAME against "DSS_INTERNAL" while DSS_INTERNAL is the deploymentMode.
import { describe, expect, it, } from "bun:test";
import { selectMlflowCodeEnvironment, } from "./live-capabilities.js";

describe("selectMlflowCodeEnvironment", () => {
	it("skips internal deploymentMode envs that ship mlflow and blocks when nothing else qualifies", () => {
		// Live names: internal rows carry arbitrary envName values; the only
		// discriminating field is deploymentMode.
		const environments = [
			{ envName: "INTERNAL_retrieval_augmented_generation_v1", deploymentMode: "DSS_INTERNAL", },
			{ envName: "default_v1", deploymentMode: "DESIGN_MANAGED", },
		];
		const packages = {
			INTERNAL_retrieval_augmented_generation_v1: ["mlflow==2.0.1", "pandas",],
			default_v1: ["pandas", "scikit-learn",],
		};
		expect(selectMlflowCodeEnvironment(environments, packages,),).toBeUndefined();
	});

	it("selects an eligible env with mlflow even when an internal env ships mlflow too", () => {
		const environments = [
			{ envName: "INTERNAL_builtin_v1", deploymentMode: "DSS_INTERNAL", },
			{ envName: "ml_env", deploymentMode: "DESIGN_MANAGED", },
		];
		const packages = {
			INTERNAL_builtin_v1: ["mlflow==2.0.1",],
			ml_env: ["mlflow==2.0.1", "pandas",],
		};
		expect(selectMlflowCodeEnvironment(environments, packages,),).toBe("ml_env",);
	});

	it("matches mlflow package specs case-insensitively and only as a package token", () => {
		const environments = [
			{ envName: "not_mlflow_helper", deploymentMode: "DESIGN_MANAGED", },
			{ envName: "ml_env", deploymentMode: "DESIGN_MANAGED", },
		];
		const packages = {
			not_mlflow_helper: ["mlflow2", "not-mlflow",],
			ml_env: ["MLflow==2.0.1",],
		};
		expect(selectMlflowCodeEnvironment(environments, packages,),).toBe("ml_env",);
	});

	it("ignores candidates without a usable name", () => {
		const environments = [
			{ deploymentMode: "DESIGN_MANAGED", },
			{ envName: "ml_env", deploymentMode: "DESIGN_MANAGED", },
		];
		const packages = { ml_env: ["mlflow",], };
		expect(selectMlflowCodeEnvironment(environments, packages,),).toBe("ml_env",);
	});
});
