import axios from 'axios';

// Frontend-side lightweight API client for backend data sources
// These functions centralize calls to Postgres/Neo4j/OLAP endpoints so
// it's easy to replace mock endpoints with real ones later.

export async function fetchPostgres(queryName: string, params?: any) {
  const res = await axios.post(`/api/v1/postgres/${queryName}`, params || {});
  return res.data;
}

export async function fetchNeo4j(cypher: string) {
  const res = await axios.post(`/api/v1/neo4j/query`, { cypher });
  return res.data;
}

export async function fetchOlap(query: any) {
  const res = await axios.post(`/api/v1/olap/query`, query);
  return res.data;
}

export async function fetchGraphNodes(nodeType: string) {
  const res = await axios.get(`/api/v1/graph/nodes`, { params: { type: nodeType } });
  return res.data;
}

export default {
  fetchPostgres,
  fetchNeo4j,
  fetchOlap,
  fetchGraphNodes,
};
