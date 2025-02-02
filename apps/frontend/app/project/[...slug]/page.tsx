import { notFound } from "next/navigation";
import { cache } from "react";
import { PlasmicComponent } from "@plasmicapp/loader-nextjs";
import { PLASMIC } from "../../../plasmic-init";
import { PlasmicClientRootProvider } from "../../../plasmic-init-client";
import {
  cachedGetProjectsBySlugs,
  cachedGetCodeMetricsByProjectIds,
  cachedGetOnchainMetricsByProjectIds,
  cachedGetAllEventTypes,
} from "../../../lib/graphql/cached-queries";
import { logger } from "../../../lib/logger";
import { catchallPathToString } from "../../../lib/paths";

const PLASMIC_COMPONENT = "ProjectPage";
//export const dynamic = STATIC_EXPORT ? "force-static" : "force-dynamic";
export const dynamic = "force-static";
export const dynamicParams = true;
export const revalidate = false; // 3600 = 1 hour
// TODO: This cannot be empty due to this bug
// https://github.com/vercel/next.js/issues/61213
const STATIC_EXPORT_SLUGS: string[] = ["opensource-observer"];
export async function generateStaticParams() {
  return STATIC_EXPORT_SLUGS.map((s) => ({
    slug: [s],
  }));
}

const cachedFetchComponent = cache(async (componentName: string) => {
  try {
    const plasmicData = await PLASMIC.fetchComponentData(componentName);
    return plasmicData;
  } catch (e) {
    logger.warn(e);
    return null;
  }
});

/**
 * This SSR route allows us to fetch the project from the database
 * on the first HTTP request, which should be faster than fetching it client-side
 */

type ProjectPageProps = {
  params: {
    slug: string[];
  };
};

export default async function ProjectPage(props: ProjectPageProps) {
  const { params } = props;
  const slugs = [catchallPathToString(params.slug)];
  if (!params.slug || !Array.isArray(params.slug) || params.slug.length < 1) {
    logger.warn("Invalid project page path", params);
    notFound();
  }

  // Get project metadata from the database
  const { projects: projectArray } = await cachedGetProjectsBySlugs({
    project_slugs: slugs,
  });
  if (!Array.isArray(projectArray) || projectArray.length < 1) {
    logger.warn(`Cannot find project (slugs=${slugs})`);
    notFound();
  }
  const project = projectArray[0];
  const projectId = project.project_id;
  //console.log("project", project);

  // Parallelize getting things related to the project
  const data = await Promise.all([
    cachedGetAllEventTypes(),
    cachedGetCodeMetricsByProjectIds({
      project_ids: [projectId],
    }),
    cachedGetOnchainMetricsByProjectIds({
      project_ids: [projectId],
    }),
  ]);
  const { event_types: eventTypes } = data[0];
  const { code_metrics_by_project: codeMetrics } = data[1];
  const { onchain_metrics_by_project: onchainMetrics } = data[2];

  // Get Plasmic component
  const plasmicData = await cachedFetchComponent(PLASMIC_COMPONENT);
  if (!plasmicData) {
    logger.warn(`Unable to get componentName=${PLASMIC_COMPONENT}`);
    notFound();
  }
  const compMeta = plasmicData.entryCompMetas[0];

  return (
    <PlasmicClientRootProvider
      prefetchedData={plasmicData}
      pageParams={compMeta.params}
    >
      <PlasmicComponent
        component={compMeta.displayName}
        componentProps={{
          metadata: project,
          codeMetrics,
          onchainMetrics,
          eventTypes,
        }}
      />
    </PlasmicClientRootProvider>
  );
}
