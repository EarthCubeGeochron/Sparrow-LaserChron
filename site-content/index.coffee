import {Markdown, useAPIResult} from '@macrostrat/ui-components'
import aboutText from './about.md'
import h from '@macrostrat/hyper'
import {DetritalZirconComponent, DZSessionData} from 'plugins/dz-spectrum'
import {AggregateHistogram} from 'plugins/dz-aggregate-histogram'
import ReactJson from "react-json-view"
import DataSheetCard from './data-sheet'

export default {
  landingText: h Markdown, {src: aboutText}
  landingGraphic: h AggregateHistogram
  siteTitle: 'Arizona LaserChron Center'
  shortSiteTitle: "ALC"
  sessionDetailTabs: [{
    title: "Detrital zircon"
    id: "detrital-zircon"
    component: DetritalZirconComponent
  }],
  sessionCardContent: ({data})-> h(DZSessionData, data),
  dataFilePage: ({data})->
    {file_hash} = data
    h(DataSheetCard, {uuid: file_hash})
}
