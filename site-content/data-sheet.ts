import { hyperStyled } from "@macrostrat/hyper";
import { Card } from "@blueprintjs/core";
import { useAPIv2Result } from "sparrow/api-v2";
import BasicDataSheet from "@earthdata/sheet/src/basic-sheet";
import styles from "./main.module.styl";
const h = hyperStyled(styles);

export default function DataSheet({ uuid }: { uuid: string }) {
  const res = useAPIv2Result(`/data_file/${uuid}/csv_data`);
  return h([
    h("h3", "Summary sheet data"),
    h(
      "p",
      "Data extracted from the AgeCalc summary sheet for further import processing."
    ),
    h(
      Card,
      {
        className: "data-sheet-card",
      },
      h(
        "div.data-sheet-container",
        null,
        h.if(res != null)(BasicDataSheet, {
          data: res,
          className: "file-data-sheet",
        })
      )
    ),
  ]);
}
