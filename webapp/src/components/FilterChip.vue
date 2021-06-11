<template>
  <a>
    <div class="filter-chip">
      <div class="filter-chip-key">{{ filter.parameter }}</div>

      <div class="filter-chip-operator">{{ filter.operator }}</div>

      <span class="filter-chip-value">
          {{ filter.value }}
        </span>

      <Icon type="ios-close-circle-outline" class="filter-chip-key-right" @click="remove"/>

    </div>
  </a>
</template>

<script>
export default {
  props: {
    filter: Object
  },
  methods: {
    onItemSelected(name) {
      if (name === "delete") {
        this.$store.commit("REMOVE_FILTER", this.filter);
      }
    },
    remove: function () {
      this.$store.commit("REMOVE_FILTER", this.filter);
    }
  }
};
</script>

<style scoped>
.filter-chip-key {
  display: inline-block;
  padding-right: 5px;
  padding-left: 5px;
  height: 24px;
  line-height: 24px;
  border-top-left-radius: 8px;
  border-bottom-left-radius: 8px;
  color: #fff;
  background-color: rgb(70, 70, 70);
}

.filter-chip-key-right {
  display: inline-block;
  height: 24px;
  line-height: 24px;
  border-top-right-radius: 8px;
  border-bottom-right-radius: 8px;
  color: red;
  padding-right: 5px;
  padding-left: 5px;
  background-color: rgb(70, 70, 70);
  vertical-align: -.09em;
}

.filter-chip-key-right:hover {
  color: #fff;
  background-color: firebrick;
}

.filter-chip-operator {
  display: inline-block;
  padding: 0 5px;
  height: 24px;
  line-height: 24px;
  color: #fff;
  background-color: rgb(33, 94, 135);
}

.filter-chip-value {
  color: #fff;
}

.filter-chip {
  margin: 5px 0 0 5px;
  background-color: rgb(70, 70, 70);
  color: #fff;
  display: inline-block;
  height: 24px;
  font-size: 12px;
  line-height: 24px;
  border-radius: 8px;
}
</style>