<template>
  <div id="app" class="container my-3">
    <h3>Github Repositories</h3>

    <div class="card mt-3">
      <div class="card-header">Repositories</div>
      <div class="card-body">
        <div class="input-group input-group-sm">
          <button class="btn btn-sm btn-primary" v-for="(repo, i) in repositories" :key="i">
            <p @click="getStats(repo)">{{repo}}</p>
          </button>
        </div><br/>
      </div>
    </div>
    <div v-if="stats">
        <p> Total commits: {{stats.totalCommits}} </p>
        <p> Total committers: {{stats.totalCommitters}} </p>
        <p> <b>Top-5:</b> </p>
        <div v-for="contributor in stats.topFiveContributors" v-bind:key="contributor.email">
            <p>{{contributor.email}}: {{contributor.commitCount}}</p>

        </div>
    </div>
  </div>
</template>

<script>
const baseURL = "http://localhost:8070";

export default {
  name: "App",
  data() {
    return {
      repositories: [],
      stats: null,
    }
  },

  mounted: function() {
    this.getRepositories();
  },

  methods: {

    async getRepositories() {
      try {
              var requestOptions = {
                  mode: 'cors',
                  method: 'GET',
                  redirect: 'follow'
              };

        const res = await fetch(`${baseURL}/repository`, requestOptions);

        if (!res.ok) {
          const message = `An error has occured: ${res.status} - ${res.statusText}`;
          throw new Error(message);
        }

        const data = await res.json();
        const result = {
          status: res.status + "-" + res.statusText,
          headers: {
            "Content-Type": res.headers.get("Content-Type"),
            "Content-Length": res.headers.get("Content-Length"),
          },
          length: res.headers.get("Content-Length"),
          data: data,
        };

        this.repositories = result.data;
      } catch (err) {
        this.stats = err.message;
      }
    },

    async getStats(repo) {
      try {
        const res = await fetch(`${baseURL}/repository/stats/` + repo);

        if (!res.ok) {
          const message = `An error has occured: ${res.status} - ${res.statusText}`;
          throw new Error(message);
        }

        const data = await res.json();

        const result = {
          status: res.status + "-" + res.statusText,
          headers: {
            "Content-Type": res.headers.get("Content-Type"),
            "Content-Length": res.headers.get("Content-Length"),
          },
          length: res.headers.get("Content-Length"),
          data: data,
        };

        this.stats = result.data;
      } catch (err) {
        this.stats = err.message;
      }
    },
  }
}
</script>

<style>
#app {
  max-width: 600px;
  margin: auto;
}
</style>
