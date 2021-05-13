<template>
  <div class="buttons-container">
    <div style="width: 120px; height: 150px; position: relative">
      <div style="display: inline-block; float: left; margin-top: 20px; width: 100%">
        <span style="margin-left: 15px; margin-right: 10px; float: left">Size</span>
        <Select
            :value="$store.getters.SIZE"
            @on-change="onPaginationSelect"
            style="display: inline-block; width: 40%; float: left"
            size="small"
            :placeholder="$store.getters.SIZE"
            elementId="dis"
        >
          <Option
              v-for="item in this.items"
              :value="item"
              :key="item"
              style="float: left; width: 20%"
          >{{ item }}
          </Option
          >
        </Select>

      </div>

      <Button
          style="margin-left: 10px; margin-top: 10px; width: 120px"
          type="primary"
          shape="circle"
          icon="ios-settings"
      >Options
      </Button
      >

      <Button
          style="margin-left: 10px; margin-top: 5px; width: 120px"
          type="primary"
          shape="circle"
          @click="request"
      >
        <Icon type="ios-mail-outline"/>
        Apply filter
      </Button>
    </div>
  </div>
</template>

<script>
export default {
  methods: {
    onPaginationSelect: function (value) {
      this.$store.commit("SET_SIZE", value);
    },
    request: function () {
      this.$store.dispatch("request");
    }
  },
  data() {
    return {
      items: [20, 50, 100],
      item: 20,
    };
  },
  mounted() {
    document.getElementById("dis").setAttribute("readonly", "true")
  }
};
</script>

<style scoped>
.buttons-container {
  width: 140px;
  height: 125px;
  position: relative;
  border-radius: 8px;
  background-color: rgb(30, 30, 30);
}
</style>

